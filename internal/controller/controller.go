package controller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	SCCName      = "openclaw-enterprise"
	SCCRoleName  = "system:openshift:scc:" + SCCName
	ManagedByKey = "app.kubernetes.io/managed-by"
	OCEManager   = "openclaw-enterprise"
	Manager      = "oce-sa-scc-controller"
	AgentKey     = "openclaw.dev/agent"
	NamespaceKey = "openclaw.dev/namespace"
	GatewayNSKey = "openclaw.dev/gateway-namespace"
)

// Eligible requires OCE's ownership and identity labels, not just a name prefix.
func Eligible(ns *corev1.Namespace, sa *corev1.ServiceAccount) bool {
	if ns.DeletionTimestamp != nil || sa.DeletionTimestamp != nil || sa.Namespace != ns.Name {
		return false
	}
	if !strings.HasPrefix(ns.Name, "oce-") || ns.Labels[ManagedByKey] != OCEManager ||
		sa.Labels[ManagedByKey] != OCEManager || sa.Labels[AgentKey] == "" || sa.Labels[NamespaceKey] == "" {
		return false
	}
	if strings.HasPrefix(sa.Name, "gateway-") && strings.HasPrefix(ns.Name, "oce-gateways-") {
		return ns.Labels[GatewayNSKey] == sa.Labels[NamespaceKey]
	}
	return strings.HasPrefix(sa.Name, "agent-") && !strings.HasPrefix(ns.Name, "oce-gateways-") &&
		ns.Labels[NamespaceKey] == sa.Labels[NamespaceKey]
}

func BindingName(saName string) string {
	name := saName + "-" + SCCName + "-scc"
	if len(name) <= 253 {
		return name
	}
	// Preserve a readable prefix while avoiding collisions for long SA names.
	digest := sha256.Sum256([]byte(saName))
	return fmt.Sprintf("%.200s-%x-%s-scc", saName, digest[:8], SCCName)
}

type Controller struct {
	client  kubernetes.Interface
	factory informers.SharedInformerFactory
	queue   workqueue.TypedRateLimitingInterface[string]
	ready   atomic.Bool
}

func New(client kubernetes.Interface) *Controller {
	c := &Controller{
		client:  client,
		factory: informers.NewSharedInformerFactory(client, 5*time.Minute),
		queue:   workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
	}
	sas := c.factory.Core().V1().ServiceAccounts().Informer()
	namespaces := c.factory.Core().V1().Namespaces().Informer()
	bindings := c.factory.Rbac().V1().RoleBindings().Informer()
	_, _ = sas.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueAccount,
		UpdateFunc: func(_, obj any) { c.enqueueAccount(obj) },
		DeleteFunc: c.enqueueAccount,
	})
	_, _ = namespaces.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueNamespace,
		UpdateFunc: func(_, obj any) { c.enqueueNamespace(obj) },
		DeleteFunc: c.enqueueNamespace,
	})
	_, _ = bindings.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueBinding,
		UpdateFunc: func(old, obj any) { c.enqueueBinding(old); c.enqueueBinding(obj) },
		DeleteFunc: c.enqueueBinding,
	})
	return c
}

func eventObject(obj any) any {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return tombstone.Obj
	}
	return obj
}

func (c *Controller) enqueueAccount(obj any) {
	sa, ok := eventObject(obj).(*corev1.ServiceAccount)
	if ok && (strings.HasPrefix(sa.Name, "agent-") || strings.HasPrefix(sa.Name, "gateway-")) {
		c.queue.Add(sa.Namespace + "/" + sa.Name)
	}
}

func (c *Controller) enqueueNamespace(obj any) {
	ns, ok := eventObject(obj).(*corev1.Namespace)
	if !ok || !strings.HasPrefix(ns.Name, "oce-") {
		return
	}
	sas, err := c.factory.Core().V1().ServiceAccounts().Informer().GetIndexer().ByIndex(cache.NamespaceIndex, ns.Name)
	if err != nil {
		slog.Error("index service accounts", "namespace", ns.Name, "error", err)
		return
	}
	for _, sa := range sas {
		c.enqueueAccount(sa)
	}
}

func (c *Controller) enqueueBinding(obj any) {
	rb, ok := eventObject(obj).(*rbacv1.RoleBinding)
	if !ok || (rb.RoleRef.Name != SCCRoleName && rb.Labels[ManagedByKey] != Manager) {
		return
	}
	for _, owner := range rb.OwnerReferences {
		if owner.APIVersion == "v1" && owner.Kind == "ServiceAccount" {
			c.queue.Add(rb.Namespace + "/" + owner.Name)
		}
	}
	for _, subject := range rb.Subjects {
		if subject.Kind == "ServiceAccount" && subject.Namespace == rb.Namespace {
			c.queue.Add(rb.Namespace + "/" + subject.Name)
		}
	}
}

func (c *Controller) Ready() bool { return c.ready.Load() }

func (c *Controller) Run(ctx context.Context) error {
	c.factory.Start(ctx.Done())
	for _, synced := range c.factory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			c.queue.ShutDown()
			return fmt.Errorf("informer cache did not sync: %w", ctx.Err())
		}
	}
	c.ready.Store(true)
	defer c.ready.Store(false)
	slog.Info("watching OCE service accounts", "scc", SCCName)
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			for c.processNext(ctx) {
			}
		})
	}
	<-ctx.Done()
	c.queue.ShutDown()
	workers.Wait()
	return nil
}

func (c *Controller) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)
	if err := c.Reconcile(ctx, key); err != nil && ctx.Err() == nil {
		slog.Error("reconcile service account; retrying", "account", key, "error", err)
		c.queue.AddRateLimited(key)
	} else {
		c.queue.Forget(key)
	}
	return true
}

func desiredBinding(sa *corev1.ServiceAccount) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: BindingName(sa.Name), Namespace: sa.Namespace,
			Labels: map[string]string{ManagedByKey: Manager},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1", Kind: "ServiceAccount", Name: sa.Name, UID: sa.UID,
			}},
		},
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: SCCRoleName},
		Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: sa.Name, Namespace: sa.Namespace}},
	}
}

func ownedBinding(rb *rbacv1.RoleBinding, sa *corev1.ServiceAccount) bool {
	if rb.Labels[ManagedByKey] != Manager {
		return false
	}
	for _, owner := range rb.OwnerReferences {
		if owner.APIVersion == "v1" && owner.Kind == "ServiceAccount" && owner.Name == sa.Name {
			return true
		}
	}
	return false
}

// Reconcile uses live reads before granting privileges; cache events only schedule work.
func (c *Controller) Reconcile(ctx context.Context, key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil || namespace == "" || name == "" {
		return fmt.Errorf("invalid service account key %q", key)
	}
	sa, err := c.client.CoreV1().ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil // Kubernetes garbage-collects bindings owned by deleted accounts.
	}
	if err != nil {
		return err
	}
	ns, err := c.client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	wanted := desiredBinding(sa)
	bindings := c.client.RbacV1().RoleBindings(namespace)
	existing, err := bindings.Get(ctx, wanted.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if !Eligible(ns, sa) {
		if existing != nil && ownedBinding(existing, sa) && existing.RoleRef == wanted.RoleRef {
			uid := existing.UID
			err = bindings.Delete(ctx, existing.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
			if apierrors.IsNotFound(err) {
				return nil
			}
			if err == nil {
				slog.Info("revoked SCC binding", "account", key)
			}
			return err
		}
		return nil
	}
	if apierrors.IsNotFound(err) {
		_, err = bindings.Create(ctx, wanted, metav1.CreateOptions{})
		if err == nil {
			slog.Info("created SCC binding", "account", key, "binding", wanted.Name)
		}
		return err
	}
	if existing.DeletionTimestamp != nil {
		return fmt.Errorf("binding %s is being deleted", wanted.Name)
	}
	if existing.RoleRef != wanted.RoleRef {
		return fmt.Errorf("binding %s already references another role; refusing to overwrite", wanted.Name)
	}
	if !ownedBinding(existing, sa) {
		if reflect.DeepEqual(existing.Subjects, wanted.Subjects) {
			return nil
		}
		return fmt.Errorf("binding %s is unmanaged and has different subjects; refusing to overwrite", wanted.Name)
	}
	if reflect.DeepEqual(existing.Subjects, wanted.Subjects) && reflect.DeepEqual(existing.OwnerReferences, wanted.OwnerReferences) {
		return nil
	}
	updated := existing.DeepCopy()
	updated.Subjects = wanted.Subjects
	updated.OwnerReferences = wanted.OwnerReferences
	_, err = bindings.Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

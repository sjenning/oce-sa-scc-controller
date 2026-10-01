package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func fixtures(gateway bool) (*corev1.Namespace, *corev1.ServiceAccount) {
	nsName, saName, identityKey := "oce-example", "agent-example", NamespaceKey
	if gateway {
		nsName, saName, identityKey = "oce-gateways-example", "gateway-example", GatewayNSKey
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: nsName, Labels: map[string]string{ManagedByKey: OCEManager, identityKey: "ns-example"},
	}}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Name: saName, Namespace: nsName, UID: types.UID("sa-original"),
		Labels: map[string]string{ManagedByKey: OCEManager, AgentKey: "agt-example", NamespaceKey: "ns-example"},
	}}
	return ns, sa
}

func TestEligible(t *testing.T) {
	tests := []struct {
		name    string
		gateway bool
		change  func(*corev1.Namespace, *corev1.ServiceAccount)
		want    bool
	}{
		{name: "agent", want: true},
		{name: "gateway", gateway: true, want: true},
		{name: "namespace name alone", change: func(n *corev1.Namespace, _ *corev1.ServiceAccount) { n.Labels = nil }},
		{name: "account name alone", change: func(_ *corev1.Namespace, s *corev1.ServiceAccount) { s.Labels = nil }},
		{name: "wrong namespace manager", change: func(n *corev1.Namespace, _ *corev1.ServiceAccount) { n.Labels[ManagedByKey] = "another-app" }},
		{name: "wrong account manager", change: func(_ *corev1.Namespace, s *corev1.ServiceAccount) { s.Labels[ManagedByKey] = "another-app" }},
		{name: "non OCE namespace", change: func(n *corev1.Namespace, s *corev1.ServiceAccount) { n.Name = "other-example"; s.Namespace = n.Name }},
		{name: "default account", change: func(_ *corev1.Namespace, s *corev1.ServiceAccount) { s.Name = "default" }},
		{name: "missing agent identity", change: func(_ *corev1.Namespace, s *corev1.ServiceAccount) { delete(s.Labels, AgentKey) }},
		{name: "missing namespace identity", change: func(_ *corev1.Namespace, s *corev1.ServiceAccount) { delete(s.Labels, NamespaceKey) }},
		{name: "different namespace identity", change: func(_ *corev1.Namespace, s *corev1.ServiceAccount) { s.Labels[NamespaceKey] = "ns-other" }},
		{name: "different gateway identity", gateway: true, change: func(n *corev1.Namespace, _ *corev1.ServiceAccount) { n.Labels[GatewayNSKey] = "ns-other" }},
		{name: "gateway in agent namespace", change: func(_ *corev1.Namespace, s *corev1.ServiceAccount) { s.Name = "gateway-example" }},
		{name: "agent in gateway namespace", gateway: true, change: func(_ *corev1.Namespace, s *corev1.ServiceAccount) { s.Name = "agent-example" }},
		{name: "different namespace", change: func(_ *corev1.Namespace, s *corev1.ServiceAccount) { s.Namespace = "oce-other" }},
		{name: "deleting account", change: func(_ *corev1.Namespace, s *corev1.ServiceAccount) { now := metav1.Now(); s.DeletionTimestamp = &now }},
		{name: "deleting namespace", change: func(n *corev1.Namespace, _ *corev1.ServiceAccount) { now := metav1.Now(); n.DeletionTimestamp = &now }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, sa := fixtures(tt.gateway)
			if tt.change != nil {
				tt.change(ns, sa)
			}
			if got := Eligible(ns, sa); got != tt.want {
				t.Fatalf("Eligible() = %v, want %v", got, tt.want)
			}
		})
	}
}

func newTestController(t *testing.T, objects ...runtime.Object) (*Controller, *fake.Clientset) {
	t.Helper()
	client := fake.NewClientset(objects...)
	c := New(client)
	t.Cleanup(c.queue.ShutDown)
	return c, client
}

func writes(client *fake.Clientset) []clienttesting.Action {
	var result []clienttesting.Action
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" || action.GetVerb() == "update" || action.GetVerb() == "delete" || action.GetVerb() == "patch" {
			result = append(result, action)
		}
	}
	return result
}

func TestReconcileCreatesOnlyAccountBindingAndIsIdempotent(t *testing.T) {
	for _, gateway := range []bool{false, true} {
		ns, sa := fixtures(gateway)
		c, client := newTestController(t, ns, sa)
		key := sa.Namespace + "/" + sa.Name
		if err := c.Reconcile(t.Context(), key); err != nil {
			t.Fatal(err)
		}
		rb, err := client.RbacV1().RoleBindings(ns.Name).Get(t.Context(), BindingName(sa.Name), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		wanted := desiredBinding(sa)
		if rb.RoleRef != wanted.RoleRef || !reflect.DeepEqual(rb.Subjects, wanted.Subjects) || !reflect.DeepEqual(rb.OwnerReferences, wanted.OwnerReferences) {
			t.Fatalf("incorrect grant: %#v", rb)
		}
		if len(writes(client)) != 1 {
			t.Fatal("expected only one binding creation")
		}
		client.ClearActions()
		if err := c.Reconcile(t.Context(), key); err != nil {
			t.Fatal(err)
		}
		if len(writes(client)) != 0 {
			t.Fatal("second reconciliation should not write")
		}
	}
}

func TestReconcileIgnoresUnrelatedAccount(t *testing.T) {
	ns, sa := fixtures(false)
	sa.Labels = nil
	c, client := newTestController(t, ns, sa)
	if err := c.Reconcile(t.Context(), ns.Name+"/"+sa.Name); err != nil {
		t.Fatal(err)
	}
	if len(writes(client)) != 0 {
		t.Fatal("unrelated account received a write")
	}
}

func TestReconcilePreservesCorrectManualBinding(t *testing.T) {
	ns, sa := fixtures(false)
	rb := desiredBinding(sa)
	rb.Labels, rb.OwnerReferences = nil, nil
	c, client := newTestController(t, ns, sa, rb)
	if err := c.Reconcile(t.Context(), ns.Name+"/"+sa.Name); err != nil {
		t.Fatal(err)
	}
	if len(writes(client)) != 0 {
		t.Fatal("manual binding should not be changed")
	}
}

func TestReconcileRefusesConflicts(t *testing.T) {
	for _, wrongRole := range []bool{false, true} {
		ns, sa := fixtures(false)
		rb := desiredBinding(sa)
		rb.Labels, rb.OwnerReferences = nil, nil
		if wrongRole {
			rb.RoleRef.Name = "cluster-admin"
		} else {
			rb.Subjects[0].Name = "unrelated-account"
		}
		c, client := newTestController(t, ns, sa, rb)
		if err := c.Reconcile(t.Context(), ns.Name+"/"+sa.Name); err == nil {
			t.Fatal("expected a binding conflict")
		}
		if len(writes(client)) != 0 {
			t.Fatal("conflicting binding must not be overwritten")
		}
	}
}

func TestReconcileRepairsManagedBinding(t *testing.T) {
	ns, sa := fixtures(false)
	rb := desiredBinding(sa)
	rb.OwnerReferences[0].UID = "sa-before-recreation"
	rb.Subjects = append(rb.Subjects, rbacv1.Subject{Kind: "ServiceAccount", Name: "unrelated", Namespace: ns.Name})
	c, client := newTestController(t, ns, sa, rb)
	if err := c.Reconcile(t.Context(), ns.Name+"/"+sa.Name); err != nil {
		t.Fatal(err)
	}
	got, err := client.RbacV1().RoleBindings(ns.Name).Get(t.Context(), rb.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Subjects, desiredBinding(sa).Subjects) || got.OwnerReferences[0].UID != sa.UID {
		t.Fatalf("binding was not repaired: %#v", got)
	}
}

func TestReconcileRevokesOnlyOwnedBindings(t *testing.T) {
	for _, owned := range []bool{false, true} {
		for _, changedNamespace := range []bool{false, true} {
			ns, sa := fixtures(false)
			rb := desiredBinding(sa)
			rb.UID = "binding-original"
			if !owned {
				rb.Labels, rb.OwnerReferences = nil, nil
			}
			if changedNamespace {
				delete(ns.Labels, ManagedByKey)
			} else {
				delete(sa.Labels, ManagedByKey)
			}
			c, client := newTestController(t, ns, sa, rb)
			if err := c.Reconcile(t.Context(), ns.Name+"/"+sa.Name); err != nil {
				t.Fatal(err)
			}
			_, err := client.RbacV1().RoleBindings(ns.Name).Get(t.Context(), rb.Name, metav1.GetOptions{})
			if owned && !apierrors.IsNotFound(err) {
				t.Fatal("ineligible managed binding was not revoked")
			}
			if !owned && err != nil {
				t.Fatal("manual binding was revoked")
			}
			if owned {
				actions := writes(client)
				if len(actions) != 1 || actions[0].GetVerb() != "delete" {
					t.Fatalf("unexpected writes: %v", actions)
				}
				opts := actions[0].(clienttesting.DeleteAction).GetDeleteOptions()
				if opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != rb.UID {
					t.Fatal("binding deletion did not guard against concurrent replacement")
				}
			}
		}
	}
}

func TestReconcileDeletedAccountAndInvalidKey(t *testing.T) {
	c, client := newTestController(t)
	if err := c.Reconcile(t.Context(), "oce-example/agent-deleted"); err != nil {
		t.Fatal(err)
	}
	if len(writes(client)) != 0 {
		t.Fatal("deleted account should not cause writes")
	}
	if err := c.Reconcile(t.Context(), "invalid-key"); err == nil {
		t.Fatal("expected an invalid key error")
	}
}

func TestBindingNameIsBoundedAndCollisionResistant(t *testing.T) {
	if BindingName("agent-example") != "agent-example-openclaw-enterprise-scc" {
		t.Fatal("ordinary binding name changed")
	}
	a := BindingName("agent-" + strings.Repeat("a", 245))
	b := BindingName("agent-" + strings.Repeat("a", 244) + "b")
	if len(a) > 253 || len(b) > 253 || a == b {
		t.Fatal("long names should be bounded and distinct")
	}
}

func startController(t *testing.T, c *Controller) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("controller shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("controller did not stop")
		}
	})
	eventually(t, c.Ready)
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("condition was not met within five seconds")
		case <-ticker.C:
		}
	}
}

func TestWatchBackfillsRetriesAndRepairsDeletedBinding(t *testing.T) {
	ns, sa := fixtures(false)
	c, client := newTestController(t, ns, sa)
	var creates atomic.Int32
	client.PrependReactor("create", "rolebindings", func(clienttesting.Action) (bool, runtime.Object, error) {
		if creates.Add(1) == 1 {
			return true, nil, apierrors.NewServiceUnavailable("temporary API failure")
		}
		return false, nil, nil
	})
	startController(t, c)
	getBinding := func() bool {
		_, err := client.RbacV1().RoleBindings(ns.Name).Get(t.Context(), BindingName(sa.Name), metav1.GetOptions{})
		return err == nil
	}
	eventually(t, getBinding)
	if creates.Load() < 2 {
		t.Fatal("failed creation was not retried")
	}
	if err := client.RbacV1().RoleBindings(ns.Name).Delete(t.Context(), BindingName(sa.Name), metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return creates.Load() >= 3 && getBinding() })
}

func TestWatchNewAccountAndNamespaceLabelChanges(t *testing.T) {
	ns, sa := fixtures(true)
	delete(ns.Labels, ManagedByKey)
	c, client := newTestController(t, ns)
	startController(t, c)
	if _, err := client.CoreV1().ServiceAccounts(ns.Name).Create(t.Context(), sa, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// Ensure the account event was reconciled before adding namespace ownership.
	eventually(t, func() bool {
		for _, a := range client.Actions() {
			if a.GetVerb() == "get" && a.GetResource().Resource == "serviceaccounts" {
				return true
			}
		}
		return false
	})
	ns.Labels[ManagedByKey] = OCEManager
	if _, err := client.CoreV1().Namespaces().Update(t.Context(), ns, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, err := client.RbacV1().RoleBindings(ns.Name).Get(t.Context(), BindingName(sa.Name), metav1.GetOptions{})
		return err == nil
	})
	delete(ns.Labels, ManagedByKey)
	if _, err := client.CoreV1().Namespaces().Update(t.Context(), ns, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		_, err := client.RbacV1().RoleBindings(ns.Name).Get(t.Context(), BindingName(sa.Name), metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	})
}

func TestReconcilePropagatesReadFailures(t *testing.T) {
	ns, sa := fixtures(false)
	c, client := newTestController(t, ns, sa)
	want := errors.New("unavailable")
	client.PrependReactor("get", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, want })
	if err := c.Reconcile(t.Context(), ns.Name+"/"+sa.Name); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
	if len(writes(client)) != 0 {
		t.Fatal("namespace read failure must not grant access")
	}
}

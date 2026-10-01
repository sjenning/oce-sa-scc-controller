# OCE ServiceAccount SCC controller

Automatically grants `openclaw-enterprise` SCC access to agent and gateway
ServiceAccounts in namespaces created by OpenClaw Enterprise (OCE). Namespace
names and ServiceAccount suffixes do not need to be configured ahead of time.

This project uses a Kubernetes watch controller. It watches ServiceAccounts,
namespaces, and RoleBindings, rather than registering an admission webhook. It
needs no HTTPS certificate or admission configuration. A controller outage does
not prevent ServiceAccount creation; reconciliation catches up after restart.

## Matching OCE resources

The matching rules come from the labels observed on the target OpenShift cluster:

| Resource | Required markers |
| --- | --- |
| Namespace | Name starts with `oce-`; `app.kubernetes.io/managed-by: openclaw-enterprise` |
| Agent namespace | Nonempty `openclaw.dev/namespace`; name does not start with `oce-gateways-` |
| Gateway namespace | Name starts with `oce-gateways-`; nonempty `openclaw.dev/gateway-namespace` |
| ServiceAccount | `app.kubernetes.io/managed-by: openclaw-enterprise`; nonempty `openclaw.dev/agent` and `openclaw.dev/namespace` |
| Agent ServiceAccount | Name starts with `agent-`; its namespace identity label matches the agent namespace's identity label |
| Gateway ServiceAccount | Name starts with `gateway-`; its namespace identity label matches the gateway namespace's identity label |

Matching names alone never grant access. `default`, `builder`, `deployer`, and
unrelated ServiceAccounts are ignored. These labels are eligibility markers:
administrators should control who can create or relabel these OCE resources.

Each matching ServiceAccount gets a namespace-scoped RoleBinding named
`<service-account>-openclaw-enterprise-scc`, pointing to the ClusterRole
`system:openshift:scc:openclaw-enterprise`, with only that ServiceAccount as its
subject. Very long names use a readable prefix and hash to stay within 253 bytes.

The controller handles existing ServiceAccounts on startup, retries failed API
writes with backoff, and resyncs every five minutes. Namespace label changes also
trigger reconciliation. It recreates deleted managed bindings and repairs their
subjects or ownership. New bindings have a ServiceAccount owner reference, so
Kubernetes garbage-collects them when the account is deleted. If an account or
namespace stops matching, the controller deletes bindings it owns.

Correct existing bindings are left unchanged, including the two manual bindings
currently on the cluster. Those unowned bindings are not automatically revoked
or garbage-collected by this controller. Conflicting unowned bindings or bindings
pointing to another role are logged and retried without being overwritten.

Reconciliation is asynchronous. A Pod admission attempt can fail before its
binding exists. Deployment and Job controllers retry; callers creating Pods
directly must retry. An admission webhook would be a different design if the
binding must exist before ServiceAccount creation returns.

## Build and test

Requires Go 1.25 or newer. The Kubernetes dependencies match the cluster's
Kubernetes 1.35 API.

```sh
make test
make build
make image IMAGE=quay.io/sjenning/oce:wehbook
docker push quay.io/sjenning/oce:wehbook
```

The container runs without root, privilege escalation, or Linux capabilities,
with a read-only root filesystem. Its Deployment leaves `runAsUser` unset so
OpenShift can assign a UID from the controller namespace's range.

## Deploy to OpenShift

The controller deploys to `openclaw-system` by default. The namespace manifest
sets only its name, leaving existing namespace labels and policies intact.

The image defaults to `quay.io/sjenning/oce:wehbook`. To use another image, set
`images[0].newName` and `newTag` in `kustomization.yaml`. The deployed image is
pinned with `images[0].digest`; update that digest after pushing a new image,
or remove it to deploy by tag. The cluster must be able to pull the image;
for a private registry, configure an image pull secret for the controller's
ServiceAccount.

Review the complete manifest, then apply it with credentials allowed to create
SCCs, cluster RBAC, and a Deployment:

```sh
oc kustomize .
oc apply -k .
oc rollout status deployment/oce-sa-scc-controller -n openclaw-system
oc logs -n openclaw-system deployment/oce-sa-scc-controller -f
```

The manifests include the exported `openclaw-enterprise` SCC and its SCC-use
ClusterRole. The controller reads ServiceAccounts, namespaces, and RoleBindings
cluster-wide and writes namespace-scoped RoleBindings. Its explicit `bind`
permission is restricted to `system:openshift:scc:openclaw-enterprise`; it has no
wildcard `bind`, `escalate`, or `cluster-admin` grant. It does not create SCCs or
modify ServiceAccounts at runtime.

For an actual matching account, check the resulting grant:

```sh
oc get rolebinding -n "$OCE_NAMESPACE" "$OCE_SERVICE_ACCOUNT-openclaw-enterprise-scc" -o yaml
oc auth can-i use scc/openclaw-enterprise -n "$OCE_NAMESPACE" \
  --as="system:serviceaccount:$OCE_NAMESPACE:$OCE_SERVICE_ACCOUNT" \
  --as-group=system:authenticated \
  --as-group=system:serviceaccounts \
  --as-group="system:serviceaccounts:$OCE_NAMESPACE"
```

For local development, `bin/oce-sa-scc-controller --kubeconfig /path/to/config`
runs against that cluster **and creates or updates live RoleBindings**. Local
unit and watch tests use a fake API client and do not modify the cluster.

`/healthz` checks process health; `/readyz` returns success after informer caches
sync. Logs are JSON. SIGTERM/SIGINT stop the workers and HTTP server gracefully.

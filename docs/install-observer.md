# Installing the scheduler observer

The scheduler observer is a read-only Kubernetes Deployment that watches your
cluster and serves a live UI of running workloads. It takes no scheduling
decisions, writes nothing to the cluster, and stores no state outside its
in-memory cache.

The trust boundary is the kube API contract: the bridge opens long-lived
`list+watch` streams against three resources (`nodes`, `pods`, and `batch/v1`
`jobs`), and nothing else. The rest of this document explains the install
in terms you can take to your platform team or security review.

## What runs

Two pods and one ClusterRole, in one namespace of your choosing.

| Object            | Kind                              | Purpose                                                                                   |
| ----------------- | --------------------------------- | ----------------------------------------------------------------------------------------- |
| `your-bridge`     | Deployment (1 replica)            | Maintains reflector caches; serves `GET /snapshot` (JSON) over an internal Service.       |
| `your-ui`         | Deployment (1 replica)            | Polls the bridge every 5 s; serves the UI over an internal Service.                       |
| `your-sa`         | ServiceAccount                    | Identity for the bridge. The UI does not call the kube API but shares the SA for symmetry.|
| `your-read-role`  | ClusterRole + ClusterRoleBinding  | The bridge's read contract. See below for the exact rules.                                |

No CRDs, no Secrets, no webhooks, no PVCs. A single namespace.

## API contract and RBAC

The bridge opens three long-lived watches against the API server (one each
for `nodes`, `pods`, `batch/v1 jobs`), preceded by an initial cluster-wide
LIST per resource. That is the entire URL surface it touches.

Minimum required ClusterRole:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: your-read-role
rules:
  - apiGroups: [""]
    resources: ["nodes", "pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch"]
```

What is **deliberately excluded**:

- No mutating verbs anywhere (`create`, `update`, `patch`, `delete`).
- No `pods/log`, `pods/exec`, `pods/portforward` — the observer never accesses workload logs or shells.
- No `pods/eviction`, `nodes/proxy`, `*/status` subresources.
- No `secrets`, `configmaps`, `serviceaccounts`, `events`, `leases`, `endpointslices`.
- No CRD groups (`apiextensions.k8s.io`, `kueue.x-k8s.io`, anything else).

Bind via `ClusterRoleBinding` because `nodes` are cluster-scoped and the
pod/job watches cover all namespaces.

### Verifying what the bridge actually does

If your environment has audit logging, every API call the bridge makes
appears with `user.username = system:serviceaccount:your-namespace:your-sa`.
You should see, per process lifetime:

- One `list` per resource (nodes, pods, jobs).
- One `watch` per resource (long-lived).
- A new `list`+`watch` pair on reconnect (typically on apiserver restart or
  `410 Gone`).

Anything else is a bug — report it.

### What this RBAC cannot scope

We currently support only cluster-wide reads on the three resources above.
Two scopings that some security reviews will ask for and we cannot offer
today:

- **Namespace-scoped reads.** The reflectors call cluster-scoped LIST
  endpoints (`GET /api/v1/pods`). Kubernetes RBAC authorizes those at
  cluster scope only; namespace-scoped RoleBindings would 403 the call.
  Supporting a `--namespace` flag (per-namespace reflectors) is a code
  change we are happy to do if it is a blocker; tell us.
- **Hiding specific namespaces from the API ask.** The
  `--exclude-namespace` flag filters the UI after objects arrive in the
  bridge's cache. It does not reduce what the bridge can read. If your
  policy requires that the observer's token cannot list certain
  namespaces, the per-namespace mode above is what you want.

## API server load

Per bridge process:

| Phase              | API calls                          | Notes                                                                                                          |
| ------------------ | ---------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Startup            | 3× `LIST` (one per resource)       | One un-paginated response per LIST. On a 10 k-pod cluster, the pod LIST is roughly 10–50 MB of JSON.           |
| Steady state       | 3× `WATCH` streams                 | Near-zero apiserver CPU; bandwidth ≈ events per second.                                                        |
| Reflector restart  | 3× `LIST` per restart              | On stream end or `410 Gone`. Exponential backoff capped at 60 s. Typical retry interval in steady state is rare. |

For clusters up to a few thousand pods this is negligible. For very large
clusters (≳ 20 k pods) the un-paginated LIST is the failure mode to watch.
Pagination is a small change; we will add it if asked.

Bridge memory is dominated by the reflector caches. Rough sizing: ~50 KB
per pod object, ~50 KB per job, ~5 KB per node. Defaults of `requests:
{cpu: 50m, memory: 128Mi}, limits: {memory: 512Mi}` cover 1k–5k pods
comfortably. Size up for larger.

## Image

Both Deployments use the same image. We share the tag with you out of
band; substitute it for `your-registry/scheduler:TAG`. To build from source
instead, the `Dockerfile` is at the repository root.

## Install

Names below are advisory. Change the namespace, ServiceAccount, etc., to
match your conventions; the bridge does not depend on its own pod or
namespace name. A complete, applyable manifest is in
[Appendix A](#appendix-a--minimal-example-manifest).

1. Apply the manifest with your chosen image tag substituted into both
   Deployments.

2. Confirm the bridge is reading. `kubectl -n your-namespace logs deploy/your-bridge`
   should reach `reflector ready (initial list complete)` for each of
   `node`, `pod`, and `job` within seconds. If a reflector stays
   un-ready, the most likely cause is RBAC; check the apiserver log for
   `Forbidden` against the `your-sa` ServiceAccount.

3. Smoke-test the snapshot endpoint:

   ```sh
   kubectl -n your-namespace port-forward svc/your-bridge 8080:8080
   curl -s localhost:8080/snapshot | jq '.summary'
   ```

   You should see counts of running and queued workloads.

4. Open the UI:

   ```sh
   kubectl -n your-namespace port-forward svc/your-ui 8000:80
   ```

   then open <http://localhost:8000>. The page is the customer UI — a
   live cluster view, nothing else. The dev tooling (scenario replayer,
   fake-job generator, chooser landing page) is built as separate JS
   bundles under `/dev/*`. When the example manifest below sets
   `UI_PRODUCTION=1`, the UI server 404s every `/dev/*` URL and every
   dev-only API (`/api/solvers`, `/api/solve`, `/api/generator/config`,
   `/scenarios/*`, `/state/config.json`, `/api/jobs`). Customers see
   only the customer UI.

To expose the UI to users without `kubectl port-forward`, see the next
section. Do not skip it.

## Configuration for non-default clusters

Defaults match the NVIDIA GPU Operator's labelling conventions and the
standard `nvidia.com/gpu` extended resource. Override the bridge
container's `args:` as needed:

| Flag                  | Default            | Change when                                                                            |
| --------------------- | ------------------ | -------------------------------------------------------------------------------------- |
| `--chip-resource`     | `nvidia.com/gpu`   | Your accelerator is advertised under another resource (`amd.com/gpu`, `google.com/tpu`). |
| `--chip-label`        | `accelerator`      | Your nodes label accelerator type under a different key (e.g. `cloud.google.com/gke-accelerator`). |
| `--chip-count-label`  | unset              | Per-node accelerator count should come from a label, not `status.capacity[<chip-resource>]`. |
| `--chips-annotation`  | unset              | Per-replica chip count should come from a Pod/Job annotation when the resource request is missing or zero. |
| `--exclude-namespace` | k8s/Kueue/GKE infra | Hide additional namespaces from the UI. **Display filter only — does not reduce RBAC.** |

If any of these need overriding, get them right before showing the UI to
your users. Wrong labels mean nodes appear under "unknown accelerator"
and workloads display zero chip requests — the UI being wrong is worse
than the UI being absent.

## Exposing the UI

The UI Service is `ClusterIP` and the UI itself has no authentication.
Anyone who can reach the Service can see every workload name, namespace,
node placement, and resource request in the cluster. **Do not expose the
Service directly to the internet.**

Reasonable patterns:

- **`kubectl port-forward`** for evaluation. No exposure to manage.
- **Ingress + an auth proxy of your choice** (oauth2-proxy, Pomerium,
  Cloudflare Access, your platform's standard) in front of the
  `your-ui` Service. The bridge itself does not need to be exposed —
  the UI Service is the only thing users talk to.
- **`kubectl proxy`** for ad-hoc shared access via a tunnel.

Two things to know about what the UI surfaces:

1. `/snapshot` returns all workload names, namespaces, container resource
   requests, and node placement. No env-var values, no Secret contents,
   no log lines.
2. The customer UI is built as its own JS bundle, separate from the
   developer tooling (replay viewer, scenario picker, fake-job
   generator). With `UI_PRODUCTION=1` the server 404s every `/dev/*`
   URL and every dev-only API (`/api/solvers`, `/api/solve`,
   `/api/generator/config`, `/scenarios/*`, `/state/config.json`,
   `/api/jobs`), so customers can't reach them by URL.  The dev
   bundles ship in the image but nothing loads or serves them.

The bridge itself accepts no inbound requests except `GET /snapshot`
on its HTTP port. Outbound, it talks to `kubernetes.default.svc` (the
in-cluster API server) and nothing else by default. There is no Sentry,
no telemetry, no metrics endpoint unless you explicitly configure one.

## What this install does not include

These are deferred, not designed out — each is straightforward to add
when there is a concrete need.

- **No Kueue CRD reflection.** If your cluster runs Kueue, the UI shows
  pods and jobs but not `ClusterQueue`, `LocalQueue`, or `Workload`
  resources. Kueue-defined quotas and admission state are not visible.
  This is the next planned feature; if it matters for your install,
  tell us and we will prioritize.
- **No `/metrics` endpoint.** No Prometheus surface, no `PodMonitor`.
- **No `NetworkPolicy` template.** Bring your own; egress is to
  `kubernetes.default.svc` only.
- **No `PodDisruptionBudget`.** Single-replica `Recreate` strategy.
- **No HA.** One bridge replica with in-memory state. If it dies the
  UI returns `404` on `/snapshot` until the replacement pod re-lists
  (seconds to a minute on a small cluster).
- **No multi-cluster aggregation.** One install observes one cluster.
  Multi-cluster views are a UI-level concern we will solve when there is
  a second cluster to view.

## Failure modes

- **API server unreachable on startup.** Reflectors retry with
  exponential backoff (1 s → 60 s cap). `/snapshot` returns `404` until
  the first LIST completes.
- **API server outage mid-stream.** The kube-rs watcher transparently
  re-LISTs on `410 Gone` and short network errors. The previous cached
  snapshot is served. On full reflector death the bridge marks the
  cluster unhealthy and `/snapshot` continues to serve the last good
  frame, with the bridge's logs showing the failed resource.
- **Bridge pod restart.** State is in-memory only; restart re-LISTs. The
  UI shows a brief "loading" state.
- **Image pull failure.** Standard k8s symptoms (`ErrImagePull`,
  `ImagePullBackOff`). The bridge is an ordinary Deployment.

## Uninstall

```sh
kubectl delete -f <your-manifest>
```

If your namespace is dedicated to this install:

```sh
kubectl delete namespace your-namespace
kubectl delete clusterrole your-read-role
kubectl delete clusterrolebinding your-read-role
```

There are no CRDs to clean up, no webhooks to deregister, no state outside
the cluster.

## Appendix A — Minimal example manifest

Complete, applyable. The strings below are placeholders — pick names
that fit your conventions, then find-and-replace before `kubectl apply`:

- `your-namespace` — Kubernetes namespace.
- `your-sa` — ServiceAccount (namespaced).
- `your-read-role` — ClusterRole + ClusterRoleBinding (cluster-scoped, so
  pick a name that won't collide with other tools in your cluster).
- `your-bridge`, `your-ui` — Deployment and Service names.
- `your-app` — value for the `app.kubernetes.io/name` label, applied to
  every object's `metadata.labels` and used by the Service selectors.
- `your-registry/scheduler:TAG` — the image tag we share with you, or
  one you built from the repository's `Dockerfile`.

The strings `scheduler-ui` (in the UI container's `command`) and
`/usr/local/bin/k8s-bridge` (the bridge binary path) are fixed inside
the image — don't rename them.

```yaml
---
apiVersion: v1
kind: Namespace
metadata:
  name: your-namespace
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: your-sa
  namespace: your-namespace
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: your-read-role
rules:
  - apiGroups: [""]
    resources: ["nodes", "pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: your-read-role
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: your-read-role
subjects:
  - kind: ServiceAccount
    name: your-sa
    namespace: your-namespace
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: your-bridge
  namespace: your-namespace
spec:
  replicas: 1
  strategy: { type: Recreate }
  selector:
    matchLabels:
      app.kubernetes.io/name: your-app
      app.kubernetes.io/component: bridge
  template:
    metadata:
      labels:
        app.kubernetes.io/name: your-app
        app.kubernetes.io/component: bridge
    spec:
      serviceAccountName: your-sa
      containers:
        - name: bridge
          image: your-registry/scheduler:TAG
          imagePullPolicy: IfNotPresent
          command:
            - /usr/local/bin/k8s-bridge
            - serve
            - --mode
            - observe
            # Bare cluster name (no `:context`) triggers in-cluster auth.
            - --cluster
            - observed
            - --port
            - "8080"
            - --snapshot-label
            - observed
            # Override these if your cluster uses non-default labelling:
            # - --chip-label
            # - cloud.google.com/gke-accelerator
            # - --chip-resource
            # - nvidia.com/gpu
          env:
            - name: RUST_LOG
              value: info
          ports:
            - name: http
              containerPort: 8080
          resources:
            requests: { cpu: 50m, memory: 128Mi }
            limits: { memory: 512Mi }
          readinessProbe:
            httpGet: { path: /snapshot, port: http }
            initialDelaySeconds: 5
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: your-bridge
  namespace: your-namespace
spec:
  selector:
    app.kubernetes.io/name: your-app
    app.kubernetes.io/component: bridge
  ports:
    - { name: http, port: 8080, targetPort: http }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: your-ui
  namespace: your-namespace
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: your-app
      app.kubernetes.io/component: ui
  template:
    metadata:
      labels:
        app.kubernetes.io/name: your-app
        app.kubernetes.io/component: ui
    spec:
      containers:
        - name: ui
          image: your-registry/scheduler:TAG     # same image as the bridge
          imagePullPolicy: IfNotPresent
          command: ["uv", "run", "--frozen", "--no-sync", "scheduler-ui"]
          env:
            - name: BRIDGE_SOURCES
              value: |
                [{"name":"observed","label":"Cluster","url":"http://your-bridge:8080"}]
            - name: UI_PRODUCTION
              value: "1"
            - name: PORT
              value: "8000"
          ports:
            - name: http
              containerPort: 8000
          resources:
            requests: { cpu: 25m, memory: 64Mi }
            limits: { memory: 256Mi }
          readinessProbe:
            httpGet: { path: /live, port: http }
            initialDelaySeconds: 5
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: your-ui
  namespace: your-namespace
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: your-app
    app.kubernetes.io/component: ui
  ports:
    - { name: http, port: 80, targetPort: http }
```

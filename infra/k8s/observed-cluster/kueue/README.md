# Kueue install for the observed cluster

The controller manifest is *not* vendored — we apply it directly from
the upstream release.

```sh
KUEUE_VERSION=v0.17.2
kubectl apply --server-side -f \
  https://github.com/kubernetes-sigs/kueue/releases/download/${KUEUE_VERSION}/manifests.yaml
kubectl -n kueue-system rollout status deploy/kueue-controller-manager
kubectl apply -f queues.yaml
```

`queues.yaml` defines four ResourceFlavors (one per chip pool), one
ClusterQueue covering all four, and a `default` LocalQueue in the
`default` namespace.  See the comment at the top of that file for the
admission model.

`scripts/setup-observed.sh` runs both steps in order against the
`scheduler-observed` cluster and is the supported entry point —
applying these by hand is only useful for ad-hoc poking.

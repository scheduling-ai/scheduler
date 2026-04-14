#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "==> Terraform apply"
terraform init
terraform apply
eval "$(terraform output -raw kubeconfig_command)"

echo "==> Applying K8s manifests"
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/scheduler-rbac.yaml
kubectl apply -f k8s/mock-chips.yaml

echo "==> Waiting for mock-chips DaemonSet"
kubectl rollout status daemonset/mock-chips -n scheduler-system --timeout=300s

echo "==> Mock chips per node:"
kubectl get nodes -o custom-columns='NAME:.metadata.name,H200:.status.capacity.example\.com/H200,H100:.status.capacity.example\.com/H100,A100:.status.capacity.example\.com/A100,L40S:.status.capacity.example\.com/L40S'

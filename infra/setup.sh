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

echo "==> Node chip labels:"
kubectl get nodes -L accelerator -L scheduler.example.com/chips

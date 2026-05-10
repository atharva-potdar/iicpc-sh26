#!/bin/bash
set -euo pipefail
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

echo "Applying cluster config"
kubectl apply -f infra/k8s/namespaces.yaml
kubectl apply -f infra/k8s/network-policies.yaml
kubectl apply -f infra/k8s/rbac.yaml
kubectl apply -f infra/k8s/pvc.yaml

echo "Verifying gVisor RuntimeClass"
kubectl get runtimeclass gvisor &&
  echo "RuntimeClass OK" ||
  {
    echo "ERROR: gVisor RuntimeClass missing — re-run Lima provision"
    exit 1
  }

echo "cluster-init complete"

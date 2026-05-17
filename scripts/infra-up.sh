#!/bin/bash
set -euo pipefail
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

echo "==> Ensuring KEDA is installed"
if ! helm get metadata keda -n keda &>/dev/null 2>&1; then
  helm install keda kedacore/keda -n keda --create-namespace --wait --timeout 120s
else
  helm upgrade keda kedacore/keda -n keda --wait --timeout 120s
fi

echo "==> Deploying platform via Helm"
helm upgrade --install obarena-platform infra/helm/obarena-platform/ \
  --namespace platform --create-namespace \
  -f infra/helm/obarena-platform/values-dev.yaml \
  --set image.tag=dev \
  --wait --timeout 300s

echo "==> infra-up complete"

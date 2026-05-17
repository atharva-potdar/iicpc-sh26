#!/usr/bin/env bash
set -euo pipefail

K3S_ARGS="--write-kubeconfig-mode 644 \
  --kubelet-arg=cpu-manager-policy=static \
  --kubelet-arg=reserved-cpus=0,1 \
  --kubelet-arg=cpu-manager-policy-options=full-pcpus-only=true"

echo "==> Installing / upgrading k3s with static CPU pinning..."
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="$K3S_ARGS" sh -
sudo rm -f /var/lib/kubelet/cpu_manager_state
sudo systemctl restart k3s

rc="$HOME/.$(basename "$SHELL")rc"
echo "==> Configuring KUBECONFIG..."
if ! grep -q "KUBECONFIG=/etc/rancher/k3s/k3s.yaml" "$rc"; then
  echo 'export KUBECONFIG=/etc/rancher/k3s/k3s.yaml' >>"$rc"
fi
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

echo "==> Checking Helm installation..."
if ! command -v helm &>/dev/null; then
  curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-4 | bash
else
  echo "Helm is already installed."
fi

echo "==> Checking Just installation..."
if ! command -v just &>/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | sudo bash -s -- --to /usr/bin
else
  echo "Just is already installed."
fi

echo "==> Waiting for node to be ready..."
kubectl wait --for=condition=Ready node --all --timeout=120s
echo "==> Host setup complete!"

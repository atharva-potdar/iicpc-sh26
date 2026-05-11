#!/usr/bin/env bash
set -euo pipefail

echo "==> Checking k3s installation..."
if ! command -v k3s &>/dev/null; then
  curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--write-kubeconfig-mode 644" sh -
else
  echo "k3s is already installed."
fi

rc="$HOME/.$(basename "$SHELL")rc"
echo "==> Configuring KUBECONFIG..."
if ! grep -q "KUBECONFIG=/etc/rancher/k3s/k3s.yaml" "$rc"; then
  echo 'export KUBECONFIG=/etc/rancher/k3s/k3s.yaml' >>"$rc"
fi
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

echo "==> Checking gVisor (runsc) installation..."
if ! command -v runsc &>/dev/null; then
  wget https://storage.googleapis.com/gvisor/releases/release/latest/x86_64/runsc -O /tmp/runsc
  wget https://storage.googleapis.com/gvisor/releases/release/latest/x86_64/containerd-shim-runsc-v1 -O /tmp/containerd-shim-runsc-v1
  sudo mv /tmp/runsc /usr/bin/runsc
  sudo mv /tmp/containerd-shim-runsc-v1 /usr/bin/containerd-shim-runsc-v1
  sudo chmod 755 /usr/bin/runsc
  sudo chmod 755 /usr/bin/containerd-shim-runsc-v1
else
  echo "gVisor is already installed."
fi

echo "==> Configuring containerd for gVisor..."
sudo mkdir -p /var/lib/rancher/k3s/agent/etc/containerd
until [ -f /var/lib/rancher/k3s/agent/etc/containerd/config.toml ]; do sleep 2; done

TMPL_PATH="/var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl"
if [ ! -f "$TMPL_PATH" ] || ! grep -q "io.containerd.runsc.v1" "$TMPL_PATH"; then
  sudo cp /var/lib/rancher/k3s/agent/etc/containerd/config.toml "$TMPL_PATH"
  sudo tee -a "$TMPL_PATH" >/dev/null <<'EOF'

[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"
EOF
  echo "Restarting k3s to apply containerd config..."
  sudo systemctl restart k3s
  until kubectl get nodes &>/dev/null; do sleep 2; done
else
  echo "containerd is already configured for gVisor."
fi

echo "==> Applying gVisor RuntimeClass..."
kubectl apply -f - <<'EOF'
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: gvisor
handler: runsc
EOF

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

echo "==> Host setup complete!"

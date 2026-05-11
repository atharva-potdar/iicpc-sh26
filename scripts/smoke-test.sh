#!/bin/bash
set -euo pipefail

export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

PASS=0
FAIL=0

check() {
  local label="$1"
  shift
  if "$@" &>/dev/null; then
    echo "  [PASS] $label"
    PASS=$((PASS + 1))
  else
    echo "  [FAIL] $label"
    FAIL=$((FAIL + 1))
  fi
}

echo "Namespaces"
for ns in platform builds sandboxes bots; do
  check "namespace/$ns exists" kubectl get namespace "$ns"
done

echo "Network policies"
check "builds deny-egress" kubectl get networkpolicy default-deny-egress -n builds
check "sandboxes allow-from-bots" kubectl get networkpolicy allow-ingress-from-bots -n sandboxes
check "bots restrict-egress" kubectl get networkpolicy restrict-egress -n bots

echo "RBAC"
check "sandbox-orchestrator SA" kubectl get serviceaccount sandbox-orchestrator -n platform
check "runtimeclass-reader" kubectl get clusterrole runtimeclass-reader

echo "gVisor"
kubectl delete pod gvisor-smoke --ignore-not-found &>/dev/null
kubectl run gvisor-smoke \
  --image=debian:stable-slim \
  --restart=Never \
  --overrides='{"spec":{"runtimeClassName":"gvisor"}}' \
  -- uname -r &>/dev/null
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded \
  pod/gvisor-smoke --timeout=120s &>/dev/null
KERNEL=$(kubectl logs gvisor-smoke 2>/dev/null)
kubectl delete pod gvisor-smoke --ignore-not-found &>/dev/null
if echo "$KERNEL" | grep -q gvisor; then
  echo "  [PASS] gVisor kernel: $KERNEL"
  PASS=$((PASS + 1))
else
  echo "  [FAIL] gVisor returned unexpected kernel: $KERNEL"
  FAIL=$((FAIL + 1))
fi

echo "Infra services"
check "Redpanda running" kubectl get pod -n platform -l app.kubernetes.io/name=redpanda --field-selector=status.phase=Running
check "TimescaleDB running" kubectl get pod -n platform -l app=timescaledb --field-selector=status.phase=Running
check "Redis running" kubectl get pod -n platform -l app.kubernetes.io/name=redis --field-selector=status.phase=Running
check "SeaweedFS running" kubectl get pod -n platform -l app=seaweedfs --field-selector=status.phase=Running
check "submission-api running" kubectl get pod -n platform -l app=submission-api --field-selector=status.phase=Running
check "build-service running" kubectl get pod -n platform -l app=build-service --field-selector=status.phase=Running

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && echo "Stack is healthy." || {
  echo "Fix failures before coding."
  exit 1
}

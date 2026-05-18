#!/bin/bash
set -euo pipefail

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

echo "Infra services"
check "Redpanda running" kubectl get pod -n platform -l app.kubernetes.io/name=redpanda --field-selector=status.phase=Running
check "TimescaleDB running" kubectl get pod -n platform -l app.kubernetes.io/name=timescaledb --field-selector=status.phase=Running
check "Redis running" kubectl get pod -n platform -l app.kubernetes.io/name=redis --field-selector=status.phase=Running
check "SeaweedFS running" kubectl get pod -n platform -l app.kubernetes.io/name=seaweedfs --field-selector=status.phase=Running
check "submission-api running" kubectl get pod -n platform -l app.kubernetes.io/name=submission-api --field-selector=status.phase=Running
check "build-service running" kubectl get pod -n platform -l app.kubernetes.io/name=build-service --field-selector=status.phase=Running
check "sandbox-orchestrator running" kubectl get pod -n platform -l app.kubernetes.io/name=sandbox-orchestrator --field-selector=status.phase=Running
check "bot-orchestrator running" kubectl get pod -n platform -l app.kubernetes.io/name=bot-orchestrator --field-selector=status.phase=Running
check "telemetry-ingester running" kubectl get pod -n platform -l app.kubernetes.io/name=telemetry-ingester --field-selector=status.phase=Running
check "leaderboard-ws running" kubectl get pod -n platform -l app.kubernetes.io/name=leaderboard-ws --field-selector=status.phase=Running

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && echo "Stack is healthy." || {
  echo "Fix failures before coding."
  exit 1
}

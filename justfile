export KUBECONFIG := "/etc/rancher/k3s/k3s.yaml"

default: dev-up

dev-up: cluster-init infra-up smoke-test

cluster-init:
    bash scripts/cluster-init.sh

infra-up:
    bash scripts/infra-up.sh

smoke-test:
    bash scripts/smoke-test.sh

dev-teardown:
    kubectl delete -f infra/k8s/platform/ || true
    kubectl delete -f infra/k8s/pvc.yaml || true
    kubectl delete -f infra/k8s/rbac.yaml || true
    kubectl delete -f infra/k8s/network-policies.yaml || true
    kubectl delete namespace platform builds sandboxes bots || true

export KUBECONFIG := "/etc/rancher/k3s/k3s.yaml"

default: dev-up

dev-up: cluster-init build infra-up smoke-test

build:
    DOCKER_BUILDKIT=1 docker build -t submission-api:dev -f services/submission-api/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t build-service:dev -f services/build-service/Dockerfile .
    docker save submission-api:dev | sudo k3s ctr images import -
    docker save build-service:dev | sudo k3s ctr images import -

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

clean-cache:
    docker image prune -a -f
    sudo k3s crictl rmi --prune

clean-cache-all:
    docker image prune -a -f
    sudo k3s crictl rmi --all

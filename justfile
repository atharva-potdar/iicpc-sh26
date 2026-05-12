export KUBECONFIG := "/etc/rancher/k3s/k3s.yaml"

default: dev-up

dev-up: cluster-init build prefetch infra-up smoke-test

build:
    DOCKER_BUILDKIT=1 docker build -t submission-api:dev -f services/submission-api/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t build-service:dev -f services/build-service/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t sandbox-orchestrator:dev -f services/sandbox-orchestrator/Dockerfile .
    docker save submission-api:dev | sudo k3s ctr images import -
    docker save build-service:dev | sudo k3s ctr images import -
    docker save sandbox-orchestrator:dev | sudo k3s ctr images import -

prefetch:
    sudo k3s crictl pull docker.redpanda.com/redpandadata/redpanda:v26.1.7
    sudo k3s crictl pull docker.io/timescale/timescaledb:latest-pg18
    sudo k3s crictl pull docker.io/library/redis:8-alpine
    sudo k3s crictl pull docker.io/chrislusf/seaweedfs:latest
    sudo k3s crictl pull docker.io/amazon/aws-cli:latest
    sudo k3s crictl pull docker.io/library/debian:stable-slim
    sudo k3s crictl pull docker.io/library/gcc:16-trixie
    sudo k3s crictl pull docker.io/library/rust:1.95-alpine
    sudo k3s crictl pull docker.io/library/golang:1.26-alpine
    sudo k3s crictl pull docker.io/library/alpine:3.23

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

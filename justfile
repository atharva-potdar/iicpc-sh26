export KUBECONFIG := "/etc/rancher/k3s/k3s.yaml"

default: dev-up

dev-up: cluster-init build prefetch infra-up smoke-test

tf-init:
    terraform -chdir=infra/terraform init

tf-plan:
    terraform -chdir=infra/terraform plan

tf-up:
    bash scripts/tf-up.sh

tf-destroy:
    terraform -chdir=infra/terraform destroy

bootstrap:
    bash scripts/bootstrap-tf-state.sh

push:
    bash scripts/push-images.sh

helm-lint:
    helm lint infra/helm/iicpc-platform/

helm-deploy:
    bash scripts/helm-deploy.sh

helm-teardown:
    helm uninstall iicpc-platform --namespace iicpc

helm-package:
    helm package infra/helm/iicpc-platform/ --destination dist/

build:
    DOCKER_BUILDKIT=1 docker build -t submission-api:dev -f services/submission-api/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t build-service:dev -f services/build-service/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t sandbox-orchestrator:dev -f services/sandbox-orchestrator/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t bot-orchestrator:dev -f services/bot-orchestrator/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t bot-runner:dev -f services/bot-runner/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t telemetry-ingester:dev -f services/telemetry-ingester/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t leaderboard-ws:dev -f services/leaderboard-ws/Dockerfile .
    docker save submission-api:dev | sudo k3s ctr images import -
    docker save build-service:dev | sudo k3s ctr images import -
    docker save sandbox-orchestrator:dev | sudo k3s ctr images import -
    docker save bot-orchestrator:dev | sudo k3s ctr images import -
    docker save bot-runner:dev | sudo k3s ctr images import -
    docker save telemetry-ingester:dev | sudo k3s ctr images import -
    docker save leaderboard-ws:dev | sudo k3s ctr images import -

prefetch:
    sudo k3s crictl pull docker.redpanda.com/redpandadata/redpanda:v26.1.7
    sudo k3s crictl pull docker.io/timescale/timescaledb:latest-pg18
    sudo k3s crictl pull docker.io/library/redis:8-alpine
    sudo k3s crictl pull docker.io/chrislusf/seaweedfs:latest
    sudo k3s crictl pull docker.io/curlimages/curl:latest
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
    kubectl delete -f infra/k8s/rbac.yaml || true
    kubectl delete -f infra/k8s/network-policies.yaml || true
    kubectl delete namespace platform builds sandboxes bots || true

clean-cache:
    docker image prune -a -f
    sudo k3s crictl rmi --prune

clean-cache-all:
    docker image prune -a -f
    sudo k3s crictl rmi --all

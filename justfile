default: dev-up

dev-up: build prefetch infra-up smoke-test

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

lint:
    golangci-lint run ./...

helm-lint:
    helm lint infra/helm/obarena-platform/

helm-deploy:
    bash scripts/helm-deploy.sh

helm-teardown:
    helm uninstall obarena-platform --namespace platform

helm-package:
    helm package infra/helm/obarena-platform/ --destination dist/

build:
    DOCKER_BUILDKIT=1 docker build -t submission-api:dev -f services/submission-api/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t build-service:dev -f services/build-service/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t sandbox-orchestrator:dev -f services/sandbox-orchestrator/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t bot-orchestrator:dev -f services/bot-orchestrator/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t bot-runner:dev -f services/bot-runner/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t telemetry-ingester:dev -f services/telemetry-ingester/Dockerfile .
    DOCKER_BUILDKIT=1 docker build -t leaderboard-ws:dev -f services/leaderboard-ws/Dockerfile .
    docker save submission-api:dev | sudo k0s ctr images import -
    docker save build-service:dev | sudo k0s ctr images import -
    docker save sandbox-orchestrator:dev | sudo k0s ctr images import -
    docker save bot-orchestrator:dev | sudo k0s ctr images import -
    docker save bot-runner:dev | sudo k0s ctr images import -
    docker save telemetry-ingester:dev | sudo k0s ctr images import -
    docker save leaderboard-ws:dev | sudo k0s ctr images import -

prefetch:
    sudo k0s crictl pull docker.redpanda.com/redpandadata/redpanda:v26.1.7
    sudo k0s crictl pull docker.io/timescale/timescaledb:latest-pg18
    sudo k0s crictl pull docker.io/library/redis:8-alpine
    sudo k0s crictl pull docker.io/chrislusf/seaweedfs:latest
    sudo k0s crictl pull docker.io/curlimages/curl:latest
    sudo k0s crictl pull docker.io/library/gcc:16-trixie
    sudo k0s crictl pull docker.io/library/rust:1.95-alpine
    sudo k0s crictl pull docker.io/library/golang:1.26-alpine
    sudo k0s crictl pull docker.io/library/alpine:3.23

infra-up:
    bash scripts/infra-up.sh

smoke-test:
    bash scripts/smoke-test.sh

dev-teardown:
    helm uninstall obarena-platform --namespace platform || true
    kubectl delete namespace platform builds sandboxes bots keda || true

clean-cache:
    docker image prune -a -f
    sudo k0s crictl rmi --prune

clean-cache-all:
    docker image prune -a -f
    sudo k0s crictl rmi --all

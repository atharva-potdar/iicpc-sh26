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
    sudo k0s ctr -n k8s.io images pull docker.redpanda.com/redpandadata/redpanda:v26.1.7
    sudo k0s ctr -n k8s.io images pull docker.io/timescale/timescaledb:latest-pg18
    sudo k0s ctr -n k8s.io images pull docker.io/library/redis:8-alpine
    sudo k0s ctr -n k8s.io images pull docker.io/chrislusf/seaweedfs:latest
    sudo k0s ctr -n k8s.io images pull docker.io/curlimages/curl:latest
    sudo k0s ctr -n k8s.io images pull docker.io/library/gcc:16-trixie
    sudo k0s ctr -n k8s.io images pull docker.io/library/rust:1.95-alpine
    sudo k0s ctr -n k8s.io images pull docker.io/library/golang:1.26-alpine
    sudo k0s ctr -n k8s.io images pull docker.io/library/alpine:3.23
    # Longhorn Core
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/longhorn-manager:v1.11.2
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/longhorn-engine:v1.11.2
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/longhorn-instance-manager:v1.11.2
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/longhorn-share-manager:v1.11.2
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/longhorn-ui:v1.11.2
    # Longhorn CSI Helpers
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/csi-provisioner:v5.3.0-20260428
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/csi-attacher:v4.11.0-20260428
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/csi-resizer:v2.1.0-20260428
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/csi-snapshotter:v8.5.0-20260428
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/csi-node-driver-registrar:v2.16.0-20260428
    sudo k0s ctr -n k8s.io images pull docker.io/longhornio/livenessprobe:v2.18.0-20260428
    # Cilium CNI (v1.19.4)
    sudo k0s ctr -n k8s.io images pull quay.io/cilium/cilium:v1.19.4
    sudo k0s ctr -n k8s.io images pull quay.io/cilium/operator-generic:v1.19.4
    sudo k0s ctr -n k8s.io images pull docker.io/alpine/curl:8.9.1

infra-up:
    bash scripts/infra-up.sh

smoke-test:
    bash scripts/smoke-test.sh

dev-teardown:
    helm uninstall obarena-platform --namespace platform --wait --timeout 120s || true
    helm uninstall keda --namespace keda || true
    k0s kubectl delete apiservice v1beta1.external.metrics.k8s.io || true
    k0s kubectl delete namespace platform builds sandboxes bots keda || true
    @echo "==> Waiting for namespaces to fully terminate..."
    @for ns in platform builds sandboxes bots keda; do \
        k0s kubectl wait --for=delete namespace/$$ns --timeout=120s 2>/dev/null || true; \
    done
    @echo "==> Teardown complete"

clean-cache:
    docker image prune -a -f
    sudo k0s crictl rmi --prune

clean-cache-all:
    docker image prune -a -f
    sudo k0s crictl rmi --all

port-forward:
    @echo "==> Starting port forwards in background..."
    k0s kubectl port-forward -n platform svc/submission-api 8080:8080 > /dev/null 2>&1 & echo $$! > .pf_submission
    k0s kubectl port-forward -n platform svc/seaweedfs 8333:8333 > /dev/null 2>&1 & echo $$! > .pf_seaweedfs
    k0s kubectl port-forward -n platform svc/leaderboard-ws 8090:8090 > /dev/null 2>&1 & echo $$! > .pf_leaderboard
    @echo "==> Port forwards active: 8080 (submission), 8333 (S3), 8090 (leaderboard)"

stop-port-forward:
    @echo "==> Stopping port forwards..."
    -kill `cat .pf_submission 2>/dev/null` 2>/dev/null || true
    -kill `cat .pf_seaweedfs 2>/dev/null` 2>/dev/null || true
    -kill `cat .pf_leaderboard 2>/dev/null` 2>/dev/null || true
    rm -f .pf_submission .pf_seaweedfs .pf_leaderboard
    @echo "==> Port forwards stopped"

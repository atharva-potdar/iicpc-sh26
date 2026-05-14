#!/bin/bash
set -euo pipefail
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

echo "==> Applying platform infra manifests"
kubectl apply -f infra/k8s/platform/

echo "==> Waiting for services to be ready"
kubectl wait --for=condition=Available deployment/seaweedfs -n platform --timeout=120s
kubectl wait --for=condition=Available deployment/redpanda -n platform --timeout=120s
kubectl wait --for=condition=Available deployment/timescaledb -n platform --timeout=120s
kubectl wait --for=condition=Available deployment/redis -n platform --timeout=120s
kubectl wait --for=condition=Available deployment/submission-api -n platform --timeout=60s
kubectl wait --for=condition=Available deployment/build-service -n platform --timeout=60s
kubectl wait --for=condition=Available deployment/sandbox-orchestrator -n platform --timeout=60s
kubectl wait --for=condition=Available deployment/bot-orchestrator -n platform --timeout=60s
kubectl wait --for=condition=Available deployment/telemetry-ingester -n platform --timeout=60s
kubectl wait --for=condition=Available deployment/leaderboard-ws -n platform --timeout=60s

echo "==> Creating SeaweedFS buckets"
kubectl delete pod seaweedfs-init -n platform --ignore-not-found --wait
kubectl run seaweedfs-init -n platform \
  --image=curlimages/curl:latest \
  --restart=Never \
  --command -- curl -X PUT \
  http://seaweedfs.platform.svc.cluster.local:8333/submissions
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded \
  pod/seaweedfs-init -n platform --timeout=60s
kubectl delete pod seaweedfs-init -n platform --wait

kubectl delete pod seaweedfs-init-builds -n platform --ignore-not-found --wait
kubectl run seaweedfs-init-builds -n platform \
  --image=curlimages/curl:latest \
  --restart=Never \
  --command -- curl -X PUT \
  http://seaweedfs.platform.svc.cluster.local:8333/builds
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded \
  pod/seaweedfs-init-builds -n platform --timeout=60s
kubectl delete pod seaweedfs-init-builds -n platform --wait

echo "==> Creating Redpanda topics"
kubectl delete pod rpk-topics -n platform --ignore-not-found --wait
kubectl run rpk-topics -n platform \
  --image=docker.redpanda.com/redpandadata/redpanda:v26.1.7 \
  --restart=Never \
  --command -- /bin/bash -c "
    rpk topic create submission.lifecycle --partitions 4 --replicas 1 \
      --brokers redpanda.platform.svc.cluster.local:9092 &&
    rpk topic create bot.metrics --partitions 8 --replicas 1 \
      --brokers redpanda.platform.svc.cluster.local:9092
  "
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded \
  pod/rpk-topics -n platform --timeout=60s
kubectl delete pod rpk-topics -n platform --wait

echo "==> Applying TimescaleDB schema"
kubectl delete pod tsdb-schema -n platform --ignore-not-found --wait
kubectl run tsdb-schema -n platform \
  --image=timescale/timescaledb:latest-pg18 \
  --restart=Never \
  --env="PGPASSWORD=iicpc" \
  --command -- psql -h timescaledb -U postgres iicpc -c "
    CREATE EXTENSION IF NOT EXISTS timescaledb;
    CREATE TABLE IF NOT EXISTS telemetry_events (
      time          TIMESTAMPTZ NOT NULL,
      submission_id UUID        NOT NULL,
      bot_id        TEXT        NOT NULL,
      event_type    TEXT        NOT NULL,
      latency_us    BIGINT,
      order_id      TEXT
    );
    SELECT create_hypertable('telemetry_events', 'time', if_not_exists => TRUE);
    CREATE TABLE IF NOT EXISTS submission_scores (
      submission_id UUID    PRIMARY KEY,
      team_name     TEXT    NOT NULL,
      p50_us        BIGINT,
      p90_us        BIGINT,
      p99_us        BIGINT,
      tps           NUMERIC,
      correctness   NUMERIC,
      composite     NUMERIC,
      scored_at     TIMESTAMPTZ DEFAULT NOW()
    );
  "
kubectl wait --for=jsonpath='{.status.phase}'=Succeeded \
  pod/tsdb-schema -n platform --timeout=60s
kubectl delete pod tsdb-schema -n platform --wait

echo "==> infra-up complete"

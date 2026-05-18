# OBARENA Platform

A distributed system for evaluating orderbook engine implementations. Contestants submit a matching engine, and the platform compiles it, runs it in an isolated sandbox, stress-tests it with a fleet of bots, scores it on latency and throughput, and publishes a live leaderboard.

## Getting Started

### Prerequisites

- Docker with BuildKit
- k0s (local Kubernetes)
- Helm
- Just (task runner)

Run the Ansible playbook in `site.yml` to install k0s, Cilium, and Helm. Then run `just` to build all images, load them into k0s, deploy via Helm, and run a smoke test.

That's the entire workflow: `just` brings up the platform from scratch.

## Why This Architecture

The problem is straightforward: accept code, compile it, run it, test it under load, score it, and show results — all while keeping untrusted code isolated and the pipeline scalable.

The naive approach would be a monolith that does everything sequentially. That works for ten submissions. It falls apart when multiple teams submit simultaneously and each submission needs its own build environment, its own sandbox, and its own bot fleet running concurrently.

So the system decomposes naturally into a pipeline of microservices, each responsible for one stage, communicating through an event bus. Go was the obvious choice — fast compilation, small binaries, and a standard library that handles HTTP, JSON, and concurrency without pulling in a framework.

Kubernetes handles the orchestration. k0s makes it painless to run locally — it's a single binary with no VM required, and Cilium provides eBPF networking with kube-proxy replacement. The same Helm chart that deploys to k0s deploys to EKS in production with only a values file change.

### On Sandboxing

The original design used gVisor for sandbox isolation. It was removed in favor of native container security: seccomp profiles, AppArmor, read-only root filesystems, dropped capabilities, and network policies. The tradeoff is that gVisor provides a separate kernel (runsc), while the current approach relies on the host kernel with syscall filtering. For a competitive programming context where the binary is already statically linked and runs for a bounded duration, the native approach is simpler to deploy and debug while still providing meaningful isolation.

Build pods don't run in a sandbox at all — they're isolated by namespace and network policy (no egress), and they use vetted compiler images. The risk of a malicious build escaping is accepted in favor of build compatibility; some language toolchains don't play well with additional sandboxing layers.

## The Pipeline

```
submission → submission-api → build-service → sandbox-orchestrator → bot-orchestrator → telemetry-ingester → leaderboard
```

Each arrow is a Redpanda event. Nothing talks to anything directly except through the event bus. This means any stage can scale independently, fail independently, and be replaced independently.

## The Services

### submission-api

A lightweight ingestion layer implementing a two-step pre-signed S3 upload flow. On `POST /submissions`, it validates the input metadata (language, team name) and returns a pre-signed SeaweedFS S3 upload URL. Once the client uploads the source archive directly to SeaweedFS, it confirms via `POST /submissions/{id}/confirm` which publishes a `submission.created` event to Redpanda. That's it — no direct file handling, no build logic, and no status tracking.

### build-service

Listens for `submission.created` events. Downloads the source from SeaweedFS, spins up an isolated build pod in the `builds` namespace (no network egress), compiles the code using a language-specific compiler image, uploads the resulting binary back to SeaweedFS, and publishes `build.complete` or `build.failed`.

The build pod uses `trap 'exit 0' TERM; sleep infinity & wait $!` as its entrypoint so it exits cleanly when deleted, rather than showing as Failed in `kubectl get pods`.

### sandbox-orchestrator

Listens for `build.complete` events. Creates a sandbox pod in the `sandboxes` namespace with an InitContainer that downloads the compiled binary from SeaweedFS. The pod runs with strict security: non-root user, read-only filesystem, dropped capabilities, seccomp profile, AppArmor, and no service account token. Waits for the pod's readiness probe (`/healthz`) to pass, then publishes `sandbox.ready` with the pod's IP.

### bot-orchestrator

Listens for `sandbox.ready` events. Spawns a bot-runner Job in the `bots` namespace pointed at the sandbox pod's WebSocket endpoint. The bot-runner runs two phases: a correctness validation (deterministic order sequences with assertions) and a load test (concurrent bots sending orders at high TPS). When the Job completes, the bot-orchestrator collects the results, deletes the sandbox pod, and publishes `test.complete`.

Also exposes `POST /run` and `GET /status` HTTP endpoints for manual test runs without going through the full pipeline.

### bot-runner

The executable that runs inside the bot-runner Job. Phase 1 sends five deterministic order sequences and validates the matching engine's responses against expected outcomes (33 assertions total). Phase 2 spawns N bot goroutines that hammer the engine with orders for a fixed duration, measuring ack/fill latency via hdrhistogram and computing throughput. Publishes results to the `bot.metrics` topic.

### telemetry-ingester

Consumes `bot.metrics` events. Computes a composite score from latency percentiles, throughput, and correctness. Writes raw telemetry to TimescaleDB for historical analysis and updates a Redis sorted set for the live leaderboard. Also publishes to a Redis pub/sub channel so the frontend gets real-time updates without polling.

### leaderboard-ws

Serves a WebSocket endpoint that the browser frontend connects to. On connect, it reads the current leaderboard from Redis (`ZREVRANGE`) and pushes it to the client. Then it subscribes to the Redis pub/sub channel and forwards live score updates as they arrive. Also serves a static HTML/JS dashboard.

## Infrastructure

### Redpanda

The event bus. All inter-service communication flows through it. Redpanda was chosen over Kafka because it's a single binary with no ZooKeeper dependency, making it trivial to deploy in both dev and prod. It's Kafka-protocol compatible, so the franz-go client works without modification.

### SeaweedFS

S3-compatible object storage for source artifacts and compiled binaries. Chosen over MinIO because it's lighter weight and handles small files well — important when every submission is a separate tar.gz.

### Redis

Two roles: a sorted set for the leaderboard (scored by composite score, ranked in descending order) and a pub/sub channel for pushing live updates to the frontend.

### TimescaleDB

PostgreSQL with the TimescaleDB extension. Stores raw telemetry events and computed submission scores. The hypertable on `telemetry_events` makes time-range queries efficient for historical analysis.

## Deployment

### Development

```bash
just          # build → deploy → smoke test
just dev-teardown   # tear everything down
```

### Production

```bash
just tf-init  # initialize Terraform
just tf-up    # provision EKS, node groups, VPC
just push     # push images to ECR
just helm-deploy   # deploy via Helm with prod values
```

The Terraform provisions an EKS cluster with three node groups (platform, sandbox, bots), each with its own taints and instance types. The Helm chart deploys all services with production resource limits, HPA configurations, and network policies.

## Scoring

The composite score is a weighted combination of three dimensions:

- **Latency** (35%) — based on ack p99 latency, normalized against a ceiling of 100ms
- **Throughput** (35%) — orders per second, normalized against a ceiling of 10,000 TPS
- **Correctness** (30%) — derived from the Phase 1 assertion pass rate (33 assertions across 5 sequences)

Each dimension is clamped to [0, 1]. The final score is in [0, 1], where higher is better.

## Project Structure

```
services/
  submission-api/        HTTP ingestion service
  build-service/         Compilation orchestrator
  sandbox-orchestrator/  Sandbox deployment orchestrator
  bot-orchestrator/      Bot fleet orchestrator
  bot-runner/            Load test executor
  telemetry-ingester/    Scoring and storage
  leaderboard-ws/        WebSocket leaderboard + frontend
  stub/                  Reference matching engine (for testing)
infra/
  helm/obarena-platform/  Helm chart (single source of truth)
  terraform/              EKS provisioning
scripts/                  Setup and deployment scripts
docs/                     Per-service documentation
```

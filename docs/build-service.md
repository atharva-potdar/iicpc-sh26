# Build Service

## Purpose

Consumes `submission.created` events from Redpanda. For each event, downloads the source artifact from SeaweedFS, spawns an isolated build pod in the `builds` namespace, compiles the code using a language-specific toolchain, uploads the resulting binary back to SeaweedFS, and publishes a lifecycle event with the result. Single responsibility — no HTTP endpoints, no authentication, no sandboxing.

## Position in Pipeline

Second service in the pipeline. Consumes from `submission.lifecycle` (after submission-api) → produces to `submission.lifecycle` (consumed by sandbox-orchestrator).

## Event Contract

**Consumer group:** `build-service`

**Reads from:** `submission.lifecycle`
**Writes to:** `submission.lifecycle`

### Consumed: submission.created

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | `"submission.created"` |
| `submission_id` | string | UUID |
| `language` | string | `"cpp"`, `"rust"`, or `"go"` |
| `team_name` | string | Team display name |
| `artifact_path` | string | `"submissions/{submission_id}.tar.gz"` |
| `created_at` | int64 | Unix nanoseconds |

### Produced: build.complete

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | `"build.complete"` |
| `submission_id` | string | UUID |
| `binary_path` | string | `"builds/{submission_id}/binary"` |
| `language` | string | Language used |
| `team_name` | string | Team display name |
| `built_at` | int64 | Unix nanoseconds |

### Produced: build.failed

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | `"build.failed"` |
| `submission_id` | string | UUID |
| `reason` | string | Compiler error output, truncated to `MAX_LOG_BYTES` |
| `failed_at` | int64 | Unix nanoseconds |

## Operational Flow

1. Consume `submission.created` event from Redpanda (consumer group: `build-service`)
2. Download source tar.gz from SeaweedFS at `event.artifact_path`
3. Validate tar.gz: no symlinks, no path traversal, valid gzip
4. Create build pod in `builds` namespace with language-specific image
5. Wait for pod to reach Running phase
6. Stream source into pod via K8s exec API (`tar xzf - -C /workspace`)
7. Execute language-specific build command via K8s exec API
8. On success:
   - Verify binary exists and is under 50MB
   - Read binary from pod via K8s exec API
   - Upload to SeaweedFS at `builds/{submission_id}/binary`
   - Publish `build.complete` event
9. On failure:
   - Publish `build.failed` event with truncated compiler stderr
10. Delete build pod (always, regardless of outcome)
11. Commit consumer offsets

## Endpoints

None. This service has no HTTP server.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `SEAWEEDFS_ENDPOINT` | `http://seaweedfs.platform.svc.cluster.local:8333` | SeaweedFS S3 endpoint |
| `REDPANDA_BROKERS` | `redpanda.platform.svc.cluster.local:9092` | Comma-separated broker list |
| `DOWNLOAD_TIMEOUT_SECONDS` | `30` | Max time to download source archive |
| `POD_START_TIMEOUT_SECONDS` | `60` | Max time to wait for build pod to reach Running |
| `INJECT_TIMEOUT_SECONDS` | `30` | Max time to inject source into pod |
| `BUILD_TIMEOUT_SECONDS` | `120` | Max time for the build command to complete |
| `UPLOAD_TIMEOUT_SECONDS` | `30` | Max time to upload binary to SeaweedFS |
| `MAX_LOG_BYTES` | `4096` | Max compiler output captured on failure |

## Dependencies

- SeaweedFS in `platform` namespace (S3-compatible storage)
- Redpanda in `platform` namespace
- Kubernetes API (in-cluster config)
- `github.com/twmb/franz-go` — Redpanda consumer/producer
- `github.com/aws/aws-sdk-go-v2` — S3 client
- `k8s.io/client-go` — Kubernetes client

## Build Toolchains

| Language | Image | Build Command |
|----------|-------|---------------|
| `cpp` | `gcc:16-trixie` | `g++ -static -O2 -o /workspace/binary /workspace/main.cpp` |
| `rust` | `rust:1.95-alpine` | `cd /workspace && RUSTFLAGS="-C target-feature=+crt-static" cargo build --release --offline && cp $(find target/release -maxdepth 1 -type f -perm -111 ! -name '*.d' \| head -1) /workspace/binary` |
| `go` | `golang:1.26-alpine` | `cd /workspace && CGO_ENABLED=0 go build -mod=vendor -o /workspace/binary .` |

All binaries are statically linked for portability.

## Build Pod Spec

| Property | Value |
|----------|-------|
| Namespace | `builds` |
| Image | Language-specific (see table above) |
| Entrypoint | `sh -c "sleep infinity & wait $!"` |
| Working directory | `/workspace` |
| Volume | EmptyDir at `/workspace`, sizeLimit 512Mi |
| CPU request | 1 |
| CPU limit | 2 |
| Memory request | 512Mi |
| Memory limit | 2Gi |

## Constraints

- Build pods run in `builds` namespace with default-deny egress NetworkPolicy
- No network access during build — all dependencies must be vendored
- Binary size limit: 50MB
- Source archive validated for symlinks and path traversal before extraction
- Build pod always deleted after completion (success or failure)
- Consumer group `build-service` ensures each submission is processed once

## RBAC

Uses the `build-service` ServiceAccount (in `platform` namespace).
Bound to `build-pod-manager` Role in `builds` namespace:

- `pods`: create, get, list, watch, delete
- `pods/exec`: create
- `pods/log`: get

## Helm Resources

| Property | Value |
|----------|-------|
| CPU request | 500m |
| CPU limit | 1000m |
| Memory request | 512Mi |
| Memory limit | 1024Mi |
| HPA | 1–4 replicas, 60% CPU target |

## TODO

- `log.Fatal` used directly in `main()` instead of `run()` helper pattern
- `log.Printf` used instead of `slog` structured logging
- Kafka topic `submission.lifecycle` hardcoded as string literal in `publisher.go`

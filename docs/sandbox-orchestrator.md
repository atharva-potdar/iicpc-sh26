# Sandbox Orchestrator

## Purpose

Consumes `build.complete` events from Redpanda. For each event, deploys the compiled binary as an isolated pod in the `sandboxes` namespace using seccomp + AppArmor sandboxing (not gVisor), waits for the submission to become healthy via readiness probe, and publishes a lifecycle event with the pod's connection details. Single responsibility — no HTTP endpoints, no authentication, no build logic.

## Position in Pipeline

Third service in the pipeline. Consumes from `submission.lifecycle` (after build-service) → produces to `submission.lifecycle` (consumed by bot-orchestrator).

## Event Contract

**Reads from:** `submission.lifecycle` (consumer group: `sandbox-orchestrator`)
**Writes to:** `submission.lifecycle`

**Consumer group:** `sandbox-orchestrator`

### Consumed: build.complete

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | `"build.complete"` |
| `submission_id` | string | UUID |
| `binary_path` | string | `"builds/{submission_id}/binary"` |
| `language` | string | Language used |
| `team_name` | string | Team display name |
| `built_at` | int64 | Unix nanoseconds |

### Produced: sandbox.ready

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | `"sandbox.ready"` |
| `submission_id` | string | UUID |
| `pod_name` | string | `"sandbox-{submission_id}"` |
| `pod_ip` | string | Pod cluster IP |
| `http_port` | int | `8080` (single port for HTTP + WebSocket) |
| `ws_port` | int | `8080` (same as http_port — single-port architecture) |
| `team_name` | string | Team display name |
| `ready_at` | int64 | Unix nanoseconds |

### Produced: sandbox.failed

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | `"sandbox.failed"` |
| `submission_id` | string | UUID |
| `reason` | string | Pod error/logs, truncated to `MAX_LOG_BYTES` |
| `failed_at` | int64 | Unix nanoseconds |

## Operational Flow

1. Consume `build.complete` event from Redpanda (consumer group: `sandbox-orchestrator`)
2. Create sandbox pod in `sandboxes` namespace with security-hardened spec
3. InitContainer (`alpine:3.23`) downloads binary from SeaweedFS via wget to EmptyDir
4. Main container (`alpine:3.23`) executes the binary directly from `/sandbox/binary`
5. Wait for pod to reach Running phase and Ready condition (via HTTP readiness probe on `/healthz`)
6. On success:
   - Get pod IP
   - Publish `sandbox.ready` event with pod IP and port (8080 for both HTTP and WebSocket)
7. On failure:
   - Collect pod logs (truncated to `MAX_LOG_BYTES`)
   - Publish `sandbox.failed` event
   - Delete sandbox pod

## Endpoints

None. This service has no HTTP server.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `SEAWEEDFS_ENDPOINT` | `http://seaweedfs.platform.svc.cluster.local:8333` | SeaweedFS S3 endpoint |
| `REDPANDA_BROKERS` | `redpanda.platform.svc.cluster.local:9092` | Comma-separated broker list |
| `SANDBOX_TIMEOUT_SECONDS` | `60` | Max time to wait for sandbox to become healthy |
| `MAX_LOG_BYTES` | `4096` | Max pod log output captured on failure |
| `SANDBOX_CPU_REQUEST` | `2` | CPU request for sandbox pods |
| `SANDBOX_CPU_LIMIT` | `2` | CPU limit for sandbox pods |
| `SANDBOX_MEMORY_REQUEST` | `512Mi` | Memory request for sandbox pods |
| `SANDBOX_MEMORY_LIMIT` | `512Mi` | Memory limit for sandbox pods |
| `SANDBOX_SECCOMP_PROFILE_PATH` | `sandbox-seccomp.json` | Seccomp profile filename |
| `SANDBOX_RUN_AS_USER` | `65534` | UID for sandbox containers (nobody) |
| `SANDBOX_NODE_SELECTOR_KEY` | `workload` | Node selector key |
| `SANDBOX_NODE_SELECTOR_VALUE` | `sandbox` | Node selector value |
| `SANDBOX_TOLERATION_KEY` | `workload` | Toleration key |
| `SANDBOX_TOLERATION_VALUE` | `sandbox` | Toleration value |

## Dependencies

- SeaweedFS in `platform` namespace (S3-compatible storage, bucket: `builds`)
- Redpanda in `platform` namespace
- Kubernetes API (in-cluster config)
- `github.com/twmb/franz-go` — Redpanda consumer/producer
- `github.com/aws/aws-sdk-go-v2` — S3 client
- `k8s.io/client-go` — Kubernetes client

## Sandbox Pod Spec

| Property | Value |
|----------|-------|
| Namespace | `sandboxes` |
| Image | `alpine:3.23` |
| Command | `/sandbox/binary` (executed directly) |
| Working directory | `/sandbox` |
| Port | `8080` (HTTP + WebSocket, single port) |
| Readiness probe | HTTP GET `/healthz` on port 8080, initial delay 1s, period 2s, failure threshold 3 |
| Restart policy | Never |
| Automount service account token | `false` |
| Node selector | `workload=sandbox` |
| Toleration | `workload=sandbox:NoSchedule` |

### Volumes

| Name | Type | Mount | Size |
|------|------|-------|------|
| `sandbox` | EmptyDir | `/sandbox` | 256Mi |
| `tmp` | EmptyDir | `/tmp` | unlimited |

### Resource Limits (Guaranteed QoS)

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | 2 | 2 |
| Memory | 512Mi | 512Mi |

### Security Context

| Property | Value |
|----------|-------|
| `runAsNonRoot` | `true` |
| `runAsUser` | `65534` (nobody) |
| `allowPrivilegeEscalation` | `false` |
| `readOnlyRootFilesystem` | `true` |
| `capabilities.drop` | `["ALL"]` |
| `seccompProfile.type` | `Localhost` |
| `seccompProfile.localhostProfile` | `sandbox-seccomp.json` |
| `appArmorProfile.type` | `RuntimeDefault` |

## Constraints

- Sandbox pods run in `sandboxes` namespace with seccomp + AppArmor isolation (no gVisor)
- Network ingress restricted to `bots` namespace only (NetworkPolicy: `allow-ingress-from-bots`)
- Network egress: only allowed to SeaweedFS in `platform` namespace on port 8333 (NetworkPolicy: `allow-seaweedfs-egress`)
- Binary must be a statically-linked executable
- Binary size limit: 50MB (inherited from build service)
- Sandbox timeout: 60 seconds (configurable) — time to reach healthy state
- One sandbox per submission
- Failed sandboxes are cleaned up automatically

## RBAC

Uses the `sandbox-orchestrator` ServiceAccount (in `platform` namespace).

Bound to `sandbox-pod-manager` Role in `sandboxes` namespace:
- `pods`: create, get, list, watch, delete
- `pods/log`: get

Bound to `runtimeclass-reader` ClusterRole (cluster-wide):
- `runtimeclasses`: get, list

## Helm Resources

| Property | Value |
|----------|-------|
| CPU request | 250m |
| CPU limit | 500m |
| Memory request | 128Mi |
| Memory limit | 256Mi |
| Autoscaling | KEDA Kafka (consumer group lag, max 4) |

## Notes

- Replaced CPU-based HPA with KEDA Kafka scaler on consumer group `sandbox-orchestrator`

## TODO

- `log.Fatal` used directly in `main()` instead of `run()` helper pattern
- `log.Printf` used instead of `slog` structured logging
- Kafka topic `submission.lifecycle` hardcoded as string literal in `publisher.go`
- S3 credentials use static `credentials.NewStaticCredentialsProvider("any", "any", "")` — should migrate to Helm-managed secrets or pre-signed URLs

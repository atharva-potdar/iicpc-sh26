# Bot Orchestrator

## Purpose

Consumes `sandbox.ready` events from Redpanda. For each event, spawns a bot runner Job in the `bots` namespace pointed at the sandbox pod's WebSocket endpoint. Waits for the Job to complete, then deletes both the sandbox pod and the bot runner Job, and publishes a `test.complete` lifecycle event. Also exposes HTTP endpoints for manual test triggering and status checking. Runs as a singleton (no HPA) to prevent duplicate test runs.

## Position in Pipeline

Fourth service in the pipeline. Consumes from `submission.lifecycle` (after sandbox-orchestrator) → produces to `submission.lifecycle` (final stage event). The bot runner Job it creates publishes to `bot.metrics` topic → consumed by telemetry-ingester.

## Event Contract

**Consumer group:** `bot-orchestrator`

**Reads from:** `submission.lifecycle`
**Writes to:** `submission.lifecycle`

### Consumed: sandbox.ready

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | `"sandbox.ready"` |
| `submission_id` | string | UUID |
| `pod_name` | string | Sandbox pod name |
| `pod_ip` | string | Sandbox pod cluster IP |
| `http_port` | int | 8080 |
| `ws_port` | int | 8080 (same as http_port) |
| `team_name` | string | Team display name |
| `ready_at` | int64 | Unix nanoseconds |

### Produced: test.complete

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | `"test.complete"` |
| `submission_id` | string | UUID |
| `team_name` | string | Team display name |
| `success` | bool | Whether the test completed successfully |
| `reason` | string | Empty on success, error message on failure |
| `completed_at` | int64 | Unix nanoseconds |

## Operational Flow

1. Consume `sandbox.ready` event from Redpanda (consumer group: `bot-orchestrator`)
2. Check `activeTests[submission_id]` — skip if already running (per-submission_id concurrency)
3. Set `activeTests[submission_id] = true`
4. Create bot runner Job in `bots` namespace:
   - Target endpoint: `ws://{pod_ip}:{ws_port}/stream`
   - Pass team name, submission ID, bot count, duration, Redpanda brokers as env vars
5. Wait 15 seconds for sandbox warmup
6. Watch Job until it succeeds or fails (timeout: `JOB_TIMEOUT_SECONDS`)
7. Delete bot runner Job from `bots` namespace
8. Delete sandbox pod from `sandboxes` namespace
9. Publish `test.complete` event
10. Delete `activeTests[submission_id]`

## Endpoints

### `POST /run`

Manually trigger a test run. Accepts optional JSON body matching `SandboxReadyEvent` schema. Fields default to:
- `submission_id`: `"manual-run"`
- `pod_ip`: `"submission-api.platform.svc.cluster.local"`
- `ws_port`: `8080`
- `team_name`: `"manual-team"`

**Response 202:** `{ "status": "started" }`
**Response 409:** `Test already in progress` (if a test is already running)

### `GET /status`

Returns current test run status.

**Response 200:** `{ "status": "idle" | "running" }`

### `GET /healthz`

**Response 200:** `ok` (plain text)

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `REDPANDA_BROKERS` | `redpanda.platform.svc.cluster.local:9092` | Comma-separated broker list |
| `NUM_BOTS` | `50` | Number of concurrent bot goroutines per runner |
| `DURATION_SECONDS` | `60` | How long to run the bot fleet |
| `JOB_TIMEOUT_SECONDS` | `120` | Max time to wait for bot runner Job to complete |
| `BOT_RUNNER_IMAGE` | `bot-runner:dev` | Container image for bot runner Jobs |

## Dependencies

- Redpanda in `platform` namespace
- Kubernetes API (in-cluster config)
- `github.com/twmb/franz-go` — Redpanda consumer/producer
- `k8s.io/client-go` — Kubernetes client

## Bot Runner Job Spec

| Property | Value |
|----------|-------|
| Namespace | `bots` |
| Image | `bot-runner:dev` (configurable via `BOT_RUNNER_IMAGE`) |
| Restart policy | Never |
| Parallelism | 1 |
| Completions | 1 |
| Backoff limit | 0 |
| TTL after finished | 300s |

### Environment Variables

| Env Var | Value |
|---------|-------|
| `TARGET_ENDPOINT` | `ws://{pod_ip}:{ws_port}/stream` |
| `NUM_BOTS` | From config (default: 50) |
| `DURATION_SECONDS` | From config (default: 60) |
| `TEAM_NAME` | From sandbox.ready event |
| `TEST_RUN_ID` | submission_id from event |
| `REDPANDA_BROKERS` | From config |

### Resource Limits

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | 100m | 2 |
| Memory | 512Mi | 1Gi |

## Constraints

- Singleton service — only one test run at a time (mutex-guarded)
- Both Kafka-triggered and HTTP-triggered runs share the same running lock
- Sandbox pod is always deleted after test, regardless of outcome
- Bot runner Job is always deleted after completion, regardless of outcome
- 15-second warmup period before Job execution begins
- `NUM_BOTS` and `DURATION_SECONDS` are platform-controlled — contestants cannot influence them

## RBAC

Uses the `bot-orchestrator` ServiceAccount (in `platform` namespace).
Bound to `bot-job-manager` Role in `bots` namespace:

- `jobs`: create, get, list, watch, delete
- `pods`: get, list, watch
- `pods/log`: get

Also deletes sandbox pods in `sandboxes` namespace (uses CoreV1().Pods() directly, no RBAC binding exists for this cross-namespace delete — see TODO).

## Helm Resources

| Property | Value |
|----------|-------|
| CPU request | 100m |
| CPU limit | 250m |
| Memory request | 64Mi |
| Memory limit | 128Mi |
| HPA | None (singleton) |

## TODO

- `log.Fatal` used directly in `main()` instead of `run()` helper pattern
- `log.Printf` used instead of `slog` structured logging
- Kafka topic `submission.lifecycle` hardcoded as string literal in `publisher.go`
- Bot orchestrator deletes sandbox pods in `sandboxes` namespace but has no RBAC binding for that namespace (only has `bot-job-manager` Role in `bots` namespace) — relies on default permissions or may fail in strict RBAC environments

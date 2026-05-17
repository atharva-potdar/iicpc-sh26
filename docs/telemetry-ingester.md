# Telemetry Ingester

## Purpose

Consumes `bot.metrics` events from Redpanda. For each event, writes raw telemetry to TimescaleDB for historical analysis and computes a composite score which is written to Redis for live leaderboard ranking. Single responsibility — no HTTP endpoints, no Kubernetes API calls, no bot logic.

## Position in Pipeline

Final processing stage. Consumes from `bot.metrics` (published by bot-runner Jobs) → writes to TimescaleDB (historical storage) and Redis (live leaderboard). Redis is then consumed by leaderboard-ws for WebSocket push to frontend.

## Event Contract

**Consumer group:** `telemetry-ingester`

**Reads from:** `bot.metrics`

### Consumed: bot.metrics

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | `"bot.metrics"` |
| `team_name` | string | Team display name |
| `submission_id` | string | UUID |
| `test_run_id` | string | Test run identifier |
| `duration_ms` | int64 | Test duration in milliseconds |
| `orders_sent` | int64 | Total orders sent during load test |
| `acks_recv` | int64 | Total acknowledgments received |
| `fills_recv` | int64 | Total fills received |
| `rejects_recv` | int64 | Total rejects received |
| `conn_drops` | int64 | Connection drops/errors |
| `ack_p50_us` | int64 | Ack latency p50 (microseconds) |
| `ack_p90_us` | int64 | Ack latency p90 (microseconds) |
| `ack_p99_us` | int64 | Ack latency p99 (microseconds) |
| `ack_p999_us` | int64 | Ack latency p99.9 (microseconds) |
| `ack_max_us` | int64 | Ack latency max (microseconds) |
| `fill_p50_us` | int64 | Fill latency p50 (microseconds) |
| `fill_p90_us` | int64 | Fill latency p90 (microseconds) |
| `fill_p99_us` | int64 | Fill latency p99 (microseconds) |
| `fill_p999_us` | int64 | Fill latency p99.9 (microseconds) |
| `fill_max_us` | int64 | Fill latency max (microseconds) |
| `tps` | float64 | Sustained transactions per second |
| `correctness_score` | float64 | Correctness validation score [0, 1] |
| `emitted_at` | int64 | Unix nanoseconds |

## Operational Flow

1. Consume `bot.metrics` event from Redpanda (consumer group: `telemetry-ingester`)
2. Compute composite score from event metrics
3. Buffer event+score pair in memory (buffer capacity: 1000)
4. Flush buffer every 500ms to TimescaleDB via `pgx.Batch`:
   - Insert into `telemetry_events` table
   - Upsert into `submission_scores` table (ON CONFLICT DO UPDATE)
5. Update Redis leaderboard via atomic Lua script:
   ```lua
   redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])         -- leaderboard, score*1000, submission_id
   redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])         -- leaderboard_details, submission_id, JSON
   return redis.call('PUBLISH', KEYS[3], ARGV[3])         -- leaderboard_updates, JSON
   ```
   Keys: `leaderboard`, `leaderboard_details`, `leaderboard_updates`
   Args: `score*1000`, `submission_id`, JSON payload
6. Log ingestion summary

## Endpoints

None. This service has no HTTP server.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `REDPANDA_BROKERS` | `redpanda.platform.svc.cluster.local:9092` | Comma-separated broker list |
| `TIMESCALEDB_DSN` | *(required, no default)* | PostgreSQL connection string |
| `REDIS_ADDR` | `redis.platform.svc.cluster.local:6379` | Redis address |
| `REDIS_PASSWORD` | *(empty)* | Redis password |
| `MAX_LATENCY_US` | `50000.0` | Latency ceiling for scoring (microseconds) |
| `MAX_TPS` | `1000.0` | TPS ceiling for scoring |

## Dependencies

- Redpanda in `platform` namespace
- TimescaleDB in `platform` namespace
- Redis in `platform` namespace
- `github.com/twmb/franz-go/pkg/kgo` — Redpanda consumer
- `github.com/jackc/pgx/v5` — TimescaleDB client
- `github.com/jackc/pgx/v5/pgxpool` — Connection pool
- `github.com/redis/go-redis/v9` — Redis client

## Scoring Formula

Composite score = `(latency_score * 0.35) + (throughput_score * 0.35) + (correctness_score * 0.30)`

### Latency Score

Weighted combination of ack latency percentiles:
```
weighted_latency_us = ack_p50 * 0.2 + ack_p90 * 0.3 + ack_p99 * 0.5
latency_score = 1 - (weighted_latency_us / MAX_LATENCY_US)
```
Clamped to [0, 1]. p99 carries the most weight (0.5) because tail latency is the primary discriminator under load.

### Throughput Score

```
throughput_score = tps / MAX_TPS
```
Clamped to [0, 1].

### Correctness Score

```
correctness_score = event.CorrectnessScore
```
Clamped to [0, 1]. Derived from the two-phase correctness validation run by bot-runner (5 deterministic sequences, 33 assertions total).

## Redis Leaderboard Mechanics

| Key | Type | Purpose |
|-----|------|---------|
| `leaderboard` | Sorted Set | Rankings by composite score * 1000; member is `submission_id` |
| `leaderboard_details` | Hash | Full JSON payload per `submission_id` |
| `leaderboard_updates` | Pub/Sub channel | Live update fan-out to WebSocket clients |

All three operations are wrapped in a single atomic Lua script to prevent partial failure.

**Score scaling:** composite score multiplied by 1000 for integer precision in Redis sorted set.

## Constraints

- Not on the critical path — slow TimescaleDB writes do not block the pipeline
- If TimescaleDB write fails, log and continue
- If Redis write fails, log and continue
- Event buffer capacity: 1000 events
- Flush interval: 500ms
- Final flush on shutdown via `doneCh` signal (flushLoop drains and closes)

## TODO

- `log.Fatal` used directly in `main()` instead of `run()` helper pattern
- `log.Printf` used instead of `slog` structured logging
- No graceful shutdown — `context.Background()` used for consumer run, no signal handling
- Consumer group offset is not explicitly committed — relies on franz-go auto-commit

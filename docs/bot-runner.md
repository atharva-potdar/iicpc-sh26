# Bot Runner

## Purpose

The executable that runs inside a Kubernetes Job spawned by bot-orchestrator. Performs two-phase testing against a contestant's submission: Phase 1 runs deterministic correctness validation sequences, Phase 2 runs a concurrent load test with multiple bot goroutines. Collects latency metrics via hdrhistogram, computes throughput, and publishes results to Redpanda `bot.metrics` topic.

## Position in Pipeline

Runs as a K8s Job in `bots` namespace. Connects to sandbox pod in `sandboxes` namespace via WebSocket → publishes to `bot.metrics` topic in Redpanda → consumed by telemetry-ingester.

## Event Contract

**Writes to:** `bot.metrics` (topic, async produce)

### Produced: bot.metrics

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | `"bot.metrics"` |
| `team_name` | string | Team display name |
| `submission_id` | string | UUID |
| `test_run_id` | string | Test run identifier |
| `duration_ms` | int64 | Load test duration in milliseconds |
| `orders_sent` | int64 | Total orders sent during load test |
| `acks_recv` | int64 | Total acks received |
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
| `correctness_score` | float64 | Phase 1 correctness score [0, 1] |
| `emitted_at` | int64 | Unix nanoseconds |

## Operational Flow

### Phase 1: Correctness Validation (synchronous, ~5-10s)

1. Open WebSocket connection to `TARGET_ENDPOINT`
2. Run 5 deterministic test sequences sequentially, one at a time
3. Each sequence sends orders/cancels synchronously, waits for responses, asserts expected behavior
4. Each assertion checks: ack received, fill received with correct price/quantity, reject received, or orderbook state matches expected
5. Compute correctness score: `passed_assertions / total_assertions`
6. Close connection after each sequence

### Phase 2: Load Test (concurrent, duration-based)

1. Create N bot goroutines (default: 10 per bot, but configured by `NUM_BOTS` from bot-orchestrator)
2. Each bot:
   - Connects to sandbox WebSocket endpoint (retries on connection refused)
   - Signals readiness via `readyCh`
   - Loops through assigned sequence templates until duration expires
   - Tracks pending orders with 5-second garbage collection timeout
   - Enforces backpressure: max 20 in-flight orders per bot
3. Wait for all bots to signal readiness (warmup complete)
4. Start measurement timer
5. Wait for all bots to complete (duration expires or error)
6. Merge per-bot metrics into aggregate
7. Compute TPS: `orders_sent / elapsed_seconds`
8. Publish aggregate metrics to Redpanda `bot.metrics` (async produce)

## Endpoints

None. This is a batch executable, not a server.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `TARGET_ENDPOINT` | `ws://localhost:8080/stream` | WebSocket endpoint of sandbox |
| `NUM_BOTS` | `10` | Number of concurrent bot goroutines |
| `DURATION_SECONDS` | `30` | Load test duration |
| `TEAM_NAME` | `unknown` | Team display name |
| `TEST_RUN_ID` | *(empty)* | Submission ID for publishing |
| `REDPANDA_BROKERS` | *(empty)* | Comma-separated broker list (empty = skip publish) |

## Correctness Sequences (Phase 1)

Five sequences run synchronously, each testing a specific aspect of orderbook behavior:

### 1. `cx_basic_match`
- Place buy limit order → verify 1 bid level on orderbook
- Place sell limit order at same price → verify fill at correct price/quantity, empty orderbook

### 2. `cx_priority_fill`
- Place buy at price P (lower) → verify 1 bid
- Place buy at price P+1 (higher) → verify 2 bids
- Place sell at price P for 10 units → verify fill at P+1 (price priority), empty orderbook

### 3. `cx_partial_fill`
- Place buy for 10 → verify 1 bid
- Place sell for 3 → verify partial fill (3 units), 1 bid remaining
- Cancel remaining bid → verify empty orderbook

### 4. `cx_cancel`
- Place buy → verify 1 bid
- Cancel buy → verify empty orderbook
- Place sell → verify 1 ask (no match since book is empty)
- Cancel sell → verify empty orderbook

### 5. `cx_reject_market_no_liquidity`
- Place market sell on empty book → verify reject with `no_liquidity` reason

Total: 33 assertions across all 5 sequences.

## Load Test Sequences (Phase 2)

Each bot is assigned one of three sequence types (based on bot ID):

### `basic_match`
- Place buy limit at base price, quantity 10
- Place sell limit at same price, quantity 10
- Expected: immediate fill

### `partial_fill_ladder`
- Place 3 buy limits at prices P, P-1, P-2 (quantity 5 each)
- Place market sell for 15 units
- Expected: sweep all 3 bid levels

### `cancel_correctness`
- Place buy limit, cancel it
- Place sell limit, cancel it
- Tests cancel path under load

Each bot uses a unique price band: `base_price = (bot_id + 1) * 1000`. Bands are 1000 units apart so that ladder sequences never overlap between bots.

## Metrics Collection

- **Ack latency**: hdrhistogram (range: 1µs to 60s, 3 significant figures)
- **Fill latency**: hdrhistogram (same range)
- **TPS**: `orders_sent / duration_seconds`
- **Connection drops**: tracked on read errors, write errors, and 5-second pending order timeout
- **Backpressure**: max 20 in-flight orders per bot; sleeps 1ms when exceeded

## Kafka Publish Flow

- Uses async produce (`client.Produce()`) with callback for error logging
- Only publishes if `TEST_RUN_ID` is non-empty and `REDPANDA_BROKERS` is configured
- Key: `submission_id`
- Falls back to logging if publish fails (does not block exit)
- Client flushed before process exit

## Constraints

- Runs as a K8s Job (not a long-running service)
- Phase 1 always runs first, regardless of Phase 2 configuration
- Each bot goroutine maintains its own WebSocket connection
- Pending orders older than 5 seconds are garbage-collected and counted as connection drops
- Backpressure limit: 20 in-flight orders per bot
- If `REDPANDA_BROKERS` is empty, metrics are not published (local testing mode)

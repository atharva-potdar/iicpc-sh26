# Leaderboard WebSocket

## Purpose

Serves the live leaderboard frontend to browsers. Reads rankings from Redis sorted set, pushes real-time updates via WebSocket to connected clients, and provides a REST endpoint for initial HTTP-based leaderboard retrieval. Also serves the embedded static frontend at `/`.

## Position in Pipeline

Reads from Redis (written by telemetry-ingester) → pushes to browser WebSocket clients. Not part of the event processing pipeline — purely a read/display service.

## Event Contract

**Reads from:** Redis sorted set `leaderboard`, hash `leaderboard_details`, pub/sub channel `leaderboard_updates`

### WebSocket Message to Client

#### Snapshot (sent on connect)

```json
{
  "type": "snapshot",
  "entries": [
    {
      "submission_id": "uuid",
      "team_name": "string",
      "score": 0.85,
      "tps": 2970.72,
      "ack_p50_us": 1445,
      "ack_p90_us": 14319,
      "ack_p99_us": 55711,
      "orders_sent": 178276,
      "rejects_recv": 0,
      "correctness": 1.0,
      "duration_ms": 60000,
      "timestamp": "2026-01-01T00:00:00Z",
      "rank": 1
    }
  ]
}
```

#### Live Update (sent on each Redis pub/sub message)

Same JSON payload as published by telemetry-ingester to `leaderboard_updates` channel — a single `leaderboardEntry` object (not wrapped in a snapshot envelope).

## Operational Flow

1. Start HTTP server, initialize Redis client
2. Start hub goroutine (manages WebSocket client connections)
3. Start Redis subscriber goroutine (fans pub/sub messages to hub)
4. Serve embedded static frontend at `GET /`
5. On WebSocket connection (`GET /ws`):
   - Build initial snapshot from Redis `ZREVRANGE leaderboard` + `HGET leaderboard_details`
   - Send snapshot to client
   - Stream live updates from hub until client disconnects
6. On HTTP request (`GET /api/leaderboard`):
   - Fetch full ranked list from Redis
   - Return JSON array ordered by rank

## Endpoints

### `GET /ws`

WebSocket endpoint for live leaderboard updates.

**On connect:** Sends full snapshot as `{"type": "snapshot", "entries": [...]}`
**On update:** Sends individual entry JSON objects as they arrive from Redis pub/sub

No client-to-server messages are expected. The connection is receive-only from the client's perspective.

### `GET /api/leaderboard`

REST endpoint returning the full leaderboard as a JSON array.

**Response 200:**
```json
[
  {
    "submission_id": "uuid",
    "team_name": "string",
    "score": 0.85,
    "tps": 2970.72,
    "ack_p50_us": 1445,
    "ack_p90_us": 14319,
    "ack_p99_us": 55711,
    "orders_sent": 178276,
    "rejects_recv": 0,
    "correctness": 1.0,
    "duration_ms": 60000,
    "timestamp": "2026-01-01T00:00:00Z",
    "rank": 1
  }
]
```

- Entries ordered by rank (best score first)
- `Access-Control-Allow-Origin: *` header set for CORS
- If `leaderboard_details` hash entry is missing, returns minimal record with only `submission_id`, `team_name`, `score`, and `rank`

### `GET /`

Serves the embedded static frontend (HTML/CSS/JS). Uses `http.FileServer` with `fs.Sub` to strip the `static` prefix.

### `GET /healthz`

**Response 200:** `{ "status": "ok" }`

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `REDIS_ADDR` | `redis.platform.svc.cluster.local:6379` | Redis address |
| `REDIS_PASSWORD` | *(empty)* | Redis password |
| `PORT` | `8090` | HTTP listen port |

## Dependencies

- Redis in `platform` namespace
- `github.com/redis/go-redis/v9` — Redis client
- `github.com/coder/websocket` — WebSocket implementation
- Embedded static frontend (Go `embed` package)

## Hub Architecture

The `Hub` manages WebSocket client connections using a channel-based fan-out pattern:

| Channel | Purpose | Buffer |
|---------|---------|--------|
| `regCh` | Client registration | 64 |
| `unregCh` | Client unregistration | 64 |
| `msgCh` | Broadcast messages | 256 |

**Client lifecycle:**
1. Connect → `hub.regCh <- client`
2. Receive snapshot → enter read loop
3. On disconnect → `hub.unregCh <- client` → `close(client.send)`

**Broadcast:** Messages from `msgCh` are fanned out to all registered clients. Slow clients (send channel full) have messages dropped rather than blocking the hub.

## Redis Data Structures

| Key | Type | Purpose |
|-----|------|---------|
| `leaderboard` | Sorted Set | Rankings by composite score * 1000; member is `submission_id` |
| `leaderboard_details` | Hash | Full JSON payload per `submission_id` |
| `leaderboard_updates` | Pub/Sub channel | Live update stream |

**Snapshot construction:**
1. `ZREVRANGE leaderboard 0 -1 WITHSCORES` — get all members in descending score order
2. Each member is the `submission_id` directly
3. `HGET leaderboard_details {submission_id}` — get full JSON payload
4. Assign rank based on position in sorted set (1-indexed)

## Constraints

- WebSocket connections are read-only from client perspective (no client messages processed)
- Slow clients may miss live updates (non-blocking send with `default` case)

## Helm Resources

| Property | Value |
|----------|-------|
| CPU request | 100m |
| CPU limit | 250m |
| Memory request | 64Mi |
| Memory limit | 128Mi |
| HPA | 2–6 replicas, 70% CPU target |


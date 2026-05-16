# Submission API

## Purpose

HTTP service accepting contestant code submissions. Validates input, stores source artifacts to SeaweedFS (S3-compatible), and publishes `submission.created` events to Redpanda to trigger the build pipeline. Single responsibility — no authentication, no status tracking, no build logic.

## Position in Pipeline

First service in the pipeline. Receives uploads from contestants → writes to SeaweedFS → publishes to Redpanda `submission.lifecycle` topic → consumed by build-service.

## Event Contract

**Writes to:** `submission.lifecycle` (topic configurable via `KAFKA_TOPIC`)

### submission.created

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | Always `"submission.created"` |
| `submission_id` | string | UUID v4 |
| `language` | string | `"cpp"`, `"rust"`, or `"go"` |
| `team_name` | string | Contestant team display name |
| `artifact_path` | string | `"submissions/{submission_id}.tar.gz"` |
| `created_at` | int64 | Unix nanoseconds |

Key: `submission_id` (ensures ordered processing per submission).

## Operational Flow

1. Receive `POST /submissions` multipart form
2. Validate: language whitelist, team_name format/length, tar.gz integrity, max upload size
3. Generate UUID submission_id
4. Upload source tar.gz to SeaweedFS at `submissions/{submission_id}.tar.gz`
5. Publish `submission.created` event to Redpanda (10s timeout)
6. On publish failure: delete orphaned S3 object, return 500
7. On success: return 202 with submission_id

## Endpoints

### `POST /submissions`

Accepts `multipart/form-data` with fields:

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `source` | file | Yes | tar.gz archive, max `MAX_UPLOAD_SIZE_MB` MiB |
| `language` | string | Yes | Must be `cpp`, `rust`, or `go` |
| `team_name` | string | Yes | Max 64 chars, alphanumeric + `-` and `_` only |

**Response 202:**
```json
{ "submission_id": "uuid" }
```

**Response 413:** `{ "error": "file too large" }`
**Response 400:** `{ "error": "unsupported language" | "team_name required" | "team_name too long" | "team_name contains invalid characters" | "missing source file" | "invalid archive" | "invalid request body" }`
**Response 500:** `{ "error": "internal error" }`

### `GET /healthz`

**Response 200:** `{ "status": "ok" }`

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `SEAWEEDFS_ENDPOINT` | `http://seaweedfs.platform.svc.cluster.local:8333` | SeaweedFS S3 endpoint |
| `REDPANDA_BROKERS` | `redpanda.platform.svc.cluster.local:9092` | Comma-separated broker list |
| `KAFKA_TOPIC` | `submission.lifecycle` | Redpanda topic name |
| `S3_BUCKET` | `submissions` | SeaweedFS bucket for source artifacts |
| `AWS_REGION` | `us-east-1` | AWS region for S3 client |
| `PORT` | `8080` | HTTP listen port |
| `MAX_UPLOAD_SIZE_MB` | `128` | Max upload size in MiB |

## Dependencies

- SeaweedFS (S3-compatible object storage) in `platform` namespace
- Redpanda (Kafka-compatible) in `platform` namespace
- `github.com/twmb/franz-go` — Redpanda producer
- `github.com/aws/aws-sdk-go-v2` — S3 client
- `github.com/google/uuid` — UUID generation

## Constraints

- Only tar.gz archives accepted (validated before upload)
- Supported languages: `cpp`, `rust`, `go`
- Max upload size: 128 MiB (configurable)
- Submission ID is UUID v4 generated server-side
- `team_name` sanitized: max 64 chars, regex `^[A-Za-z0-9_-]+$`
- `X-Request-ID` header used for request correlation (generated if absent)
- Graceful shutdown on SIGTERM/SIGINT with 15s timeout
- No deduplication — each upload gets a new submission ID
- On Kafka publish failure, orphaned S3 object is cleaned up

## TODO

- None. All known bugs in this service have been fixed.

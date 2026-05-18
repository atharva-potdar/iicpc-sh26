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

1. Receive `POST /submissions` (initialization request) containing metadata JSON.
2. Validate: language whitelist, team_name format and length.
3. Generate UUID `submission_id`.
4. Generate a SeaweedFS S3 pre-signed upload URL at `submissions/{submission_id}.tar.gz` with a 15-minute expiration time.
5. Save the initialization details in the server's pending map.
6. Return `202 Accepted` to the client along with `submission_id`, `presigned_url`, `artifact_key`, and `expires_at`.
7. The client uploads the `.tar.gz` source archive directly to the `presigned_url` (direct S3 PUT to SeaweedFS).
8. Client calls `POST /submissions/{id}/confirm` to confirm completion.
9. `submission-api` validates the session, publishes the `submission.created` event to Redpanda, and removes the entry from the pending map.

## Endpoints

### `POST /submissions` (Initialize Upload)

Accepts a JSON payload requesting a pre-signed S3 upload URL.

**Request Body:**
```json
{
  "language": "cpp" | "rust" | "go",
  "team_name": "string"
}
```

* **Constraints:**
  - `language`: Must be `cpp`, `rust`, or `go`
  - `team_name`: Max 64 chars, alphanumeric + `-` and `_` only (`^[A-Za-z0-9_-]+$`)

**Response 202:**
```json
{
  "submission_id": "uuid",
  "presigned_url": "http://...",
  "artifact_key": "submissions/uuid.tar.gz",
  "expires_at": 1785673892
}
```

**Response 400:** `{ "error": "invalid JSON" | "unsupported language" | "team_name required" | "team_name too long" | "team_name contains invalid characters" }`
**Response 500:** `{ "error": "internal error" }`

### `POST /submissions/{id}/confirm` (Confirm Upload & Trigger Pipeline)

Confirms that the source archive has been successfully uploaded to the pre-signed S3 URL and triggers the compilation pipeline.

**Response 202:**
```json
{ "submission_id": "uuid" }
```

**Response 400:** `{ "error": "submission_id required" }`
**Response 404:** `{ "error": "submission not found or expired" }`
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


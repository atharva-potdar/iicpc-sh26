# Build Service

## Overview

Consumes `submission.created` events from Redpanda. For each event, downloads
the source artifact from SeaweedFS, spawns an isolated build pod in the `builds`
namespace, compiles the code, uploads the resulting binary back to SeaweedFS,
and publishes a lifecycle event with the result.

Single responsibility — no HTTP endpoints, no auth, no sandboxing.
Runs as a single consumer in the `platform` namespace.

---

## Event Consumption

Topic:          submission.lifecycle
Consumer group: build-service
Events handled: submission.created

---

## Build Flow

1. Consume `submission.created` event
2. Download artifact from SeaweedFS at `event.artifact_path`
3. Spawn build pod in `builds` namespace:
   - Network egress denied (NetworkPolicy)
   - Pod runs `sleep infinity` as entrypoint to stay alive
   - Language-specific image and build command
   - CPU and memory limits enforced
4. Wait for pod to reach Running phase
5. Stream source tar.gz into pod via K8s exec API (`tar xzf -`)
6. Execute language-specific build command via K8s exec API
7. On success:
   - Read binary from pod via K8s exec API
   - Upload binary to SeaweedFS at `builds/{submission_id}/binary`
   - Publish `build.complete` event
8. On failure:
   - Publish `build.failed` event with compiler output (truncated to MAX_LOG_BYTES)
9. Delete build pod (always, regardless of outcome)

---

## Build Images and Commands

| Language | Image                        | Build command                                      |
|----------|------------------------------|----------------------------------------------------|
| cpp      | gcc:16-trixie                | g++ -O2 -o binary main.cpp                         |
| rust     | rust:1.95-alpine             | cargo build --release --offline                    |
| go       | golang:1.26-alpine           | go build -mod=vendor -o binary .                   |

---

## Event Schema

Topic: submission.lifecycle
Key:   submission_id

build.complete
{
  "event":          "build.complete",
  "submission_id":  "uuid",
  "binary_path":    "builds/{submission_id}/binary",
  "language":       "cpp" | "rust" | "go",
  "team_name":      "string",
  "built_at":       1234567890    // unix nanoseconds
}

build.failed
{
  "event":          "build.failed",
  "submission_id":  "uuid",
  "reason":         "string",     // compiler error output, truncated to 4KB
  "failed_at":      1234567890
}

---

## Configuration

SEAWEEDFS_ENDPOINT    SeaweedFS S3 endpoint
                      default: http://seaweedfs.platform.svc.cluster.local:8333
REDPANDA_BROKERS      comma-separated broker list
                      default: redpanda.platform.svc.cluster.local:9092
BUILD_TIMEOUT_SECONDS max time to wait for a build pod to complete
                      default: 120
MAX_LOG_BYTES         max compiler output captured on failure
                      default: 4096

---

## Constraints

- Build pods run in the `builds` namespace with default-deny egress
- Build pods have no network access — cannot download dependencies. The build pod will have no internet during the build process. Anything that is not vendored or requires internet access to build will fail the build process.
- Submissions must be the entire project, with vendored dependencies
- Binary size limit: 50MB
- Build timeout: 120 seconds (configurable)
- One build at a time per submission (no parallel builds for same ID)
- Build service uses the sandbox-orchestrator ServiceAccount for k8s API access

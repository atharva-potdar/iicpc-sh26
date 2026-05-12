# Sandbox Orchestrator

## Overview

Consumes `build.complete` events from Redpanda. For each event, downloads
the compiled binary from SeaweedFS, deploys it as a gVisor-isolated pod in the
`sandboxes` namespace, waits for the submission to become healthy, and publishes
a lifecycle event with the result.

Single responsibility — no HTTP endpoints, no auth, no build logic.
Runs as a single consumer in the `platform` namespace.

---

## Event Consumption

Topic:          submission.lifecycle
Consumer group: sandbox-orchestrator
Events handled: build.complete

---

## Deployment Flow

1. Consume `build.complete` event
2. Download binary from SeaweedFS at `event.binary_path`
3. Create sandbox pod in `sandboxes` namespace:
   - RuntimeClass: `gvisor` (runsc)
   - Network ingress allowed only from `bots` namespace (NetworkPolicy)
   - Network egress denied (NetworkPolicy)
   - Binary injected via K8s exec API
   - Exposes ports 8080 (HTTP) and 8081 (WebSocket)
   - CPU and memory limits enforced
4. Wait for pod to reach Running phase
5. Stream binary into pod via K8s exec API
6. Mark binary as executable (`chmod +x`)
7. Execute binary in background
8. Health check: poll `GET /healthz` until 200 or timeout
9. On success:
   - Publish `sandbox.ready` event with pod IP and ports
10. On failure:
    - Collect pod logs (truncated to MAX_LOG_BYTES)
    - Publish `sandbox.failed` event with reason
    - Delete sandbox pod

---

## Sandbox Pod Spec

Runtime:        gvisor (runsc)
Image:          alpine:3.23 (minimal base — binary is injected)
Entrypoint:     sleep infinity (kept alive for binary injection)
Working dir:    /sandbox
Ports:          8080 (HTTP), 8081 (WebSocket)

Resource Limits:
  CPU request:    500m
  CPU limit:      2
  Memory request: 256Mi
  Memory limit:   1Gi

Volume:
  EmptyDir at /sandbox, sizeLimit 256Mi

---

## Event Schema

Topic: submission.lifecycle
Key:   submission_id

sandbox.ready
{
  "event":          "sandbox.ready",
  "submission_id":  "uuid",
  "pod_name":       "string",
  "pod_ip":         "10.42.x.x",
  "http_port":      8080,
  "ws_port":        8081,
  "team_name":      "string",
  "ready_at":       1234567890    // unix nanoseconds
}

sandbox.failed
{
  "event":          "sandbox.failed",
  "submission_id":  "uuid",
  "reason":         "string",     // pod logs or error message, truncated to 4KB
  "failed_at":      1234567890
}

---

## Configuration

SEAWEEDFS_ENDPOINT      SeaweedFS S3 endpoint
                        default: http://seaweedfs.platform.svc.cluster.local:8333
REDPANDA_BROKERS        comma-separated broker list
                        default: redpanda.platform.svc.cluster.local:9092
SANDBOX_TIMEOUT_SECONDS max time to wait for sandbox pod to become healthy
                        default: 60
MAX_LOG_BYTES           max pod log output captured on failure
                        default: 4096
HEALTH_CHECK_INTERVAL   interval between /healthz polls
                        default: 2s
HEALTH_CHECK_RETRIES    max number of /healthz attempts before giving up
                        default: 15

---

## Security Model

- Sandbox pods run with gVisor (runsc) — kernel syscall isolation
- Network ingress restricted to `bots` namespace only (NetworkPolicy: allow-ingress-from-bots)
- Network egress fully denied (NetworkPolicy: allow-ingress-from-bots, policyTypes includes Egress with no egress rules)
- Pods run as non-root where possible
- No service account token mounted in sandbox pods
- EmptyDir with sizeLimit prevents disk abuse

---

## RBAC

Uses the `sandbox-orchestrator` ServiceAccount (platform namespace).
Bound to `sandbox-pod-manager` Role in `sandboxes` namespace:

- pods:     create, get, list, watch, delete
- pods/log: get

Also bound to `runtimeclass-reader` ClusterRole:

- runtimeclasses: get, list

---

## Constraints

- Sandbox pods run in the `sandboxes` namespace with gVisor isolation
- Sandbox pods have no outbound network access
- Sandbox pods can only receive traffic from the `bots` namespace
- Binary must be a statically-linked executable (dynamic linking may fail under gVisor)
- Binary size limit: 50MB (inherited from build service)
- Sandbox timeout: 60 seconds (configurable) — time to reach healthy state
- One sandbox per submission (previous sandbox for same submission should be cleaned up)
- Sandbox orchestrator uses the sandbox-orchestrator ServiceAccount for K8s API access

# Future Scope

## Phase 1: Event-Driven Autoscaling & Compute Isolation

### [COMPLETED] KEDA Kafka-Triggered Autoscaling

> [!NOTE]
> **Status:** Fully Implemented. Event-driven autoscaling has been successfully set up using KEDA `ScaledObjects` triggered by Redpanda consumer group lag, ensuring efficient scaling independent of CPU metrics.

**What:** Replace CPU-based HPAs on `submission-api`, `build-service`, `sandbox-orchestrator`, and `telemetry-ingester` with KEDA `ScaledObjects` using Kafka topic lag triggers on `submission.lifecycle`.

**Why:** CPU is not just a lagging indicator — it's the wrong signal entirely. A consumer goroutine blocked on `PollFetches` waiting for messages uses near-zero CPU but is perfectly healthy. CPU-based HPA would scale it down. Kafka lag is the only meaningful signal for consumer scaling: replicas scale in direct proportion to the actual backlog of submissions to process. `maxReplicaCount` must be hardcapped to the Redpanda partition count per consumer group. Since build-service and sandbox-orchestrator consume `submission.lifecycle` in different consumer groups, they can scale independently — but scaling sandbox-orchestrator beyond partition count is wasteful since extra replicas will idle.

### Node-Level Workload Isolation

**What:** Apply dedicated taints to sandbox nodes and configure `NodeAffinity` + tolerations so that platform namespace workloads (submission-api, build-service, Redpanda, TimescaleDB, Redis) are never scheduled on sandbox nodes, and vice versa.

**Why:** Platform workloads introduce noisy-neighbor effects that contaminate latency measurements. TimescaleDB compaction, Redpanda log flushes, and queue processing spikes all compete for CPU and I/O on shared nodes. Isolating sandbox pods onto dedicated nodes ensures that p50/p90/p99 latency metrics reflect only the contestant's code quality, not infrastructure contention.

---

## Phase 2: eBPF Networking, L7 Security & Upload Architecture

### Cilium CNI Replacement

**What:** Replace the default CNI (kube-proxy/iptables) with Cilium, leveraging eBPF for pod-to-pod networking and service load balancing.

**Why:** The primary driver is L7 policy enforcement and observability, not raw packet forwarding performance. At ~50 bots the iptables rule count is in the hundreds, not tens of thousands — the O(1) vs linear lookup difference is measurable but not decisive. Cilium's real advantage is the ability to enforce application-layer egress policies (preventing protocol smuggling over allowed ports) and the built-in network visibility via `cilium monitor` and Hubble. The eBPF datapath is a bonus; the L7 security and observability are the justification.

**Caveat:** Cilium in replacement mode on EKS requires disabling the AWS VPC CNI, which affects pod IP allocation. This is an installation consideration, not a blocker.

### L7 Default-Deny Egress for Sandboxes

**What:** Deploy Cilium `CiliumNetworkPolicy` with default-deny egress on all sandbox pods. Whitelist only the required destinations: CoreDNS (UDP/TCP 53), Redpanda (TCP 9092), Kubernetes API (TCP 443), and SeaweedFS (TCP 8333). All other egress is denied at L7.

**Why:** The current NetworkPolicy-based egress rules operate at L3/L4 and cannot inspect application-layer protocols. An L7 policy prevents protocol smuggling (e.g., DNS tunneling over allowed ports) and ensures sandbox pods can only communicate with platform services using the intended protocols. This is a prerequisite for any production deployment handling untrusted contestant code.

### [COMPLETED] Pre-Signed S3 Uploads

> [!NOTE]
> **Status:** Fully Implemented. The `submission-api` utilizes a two-step S3 pre-signed upload initialization (`POST /submissions`) and confirmation (`POST /submissions/{id}/confirm`) mechanism, removing the Go process from the data path and scaling uploads seamlessly.

**What:** Refactor `submission-api` `POST /submissions` to return a pre-signed SeaweedFS S3 upload URL instead of accepting the file through the Go backend. Clients upload `.tar.gz` files directly to SeaweedFS, then call a lightweight confirmation endpoint with the artifact key.

**Why deferred over direct fix:** Pre-signed URLs eliminate the need for any static AWS credentials in artifact access — the InitContainer downloads directly via a time-limited URL rather than signing with SDK credentials. This is the proper fix path instead of patching the credential chain locally.

**Why:** The current flow routes the entire file through the Go process, which must hold the upload in memory (or spill to disk) before forwarding to SeaweedFS. Under concurrent load, this causes OOMKills and disk I/O bottlenecks. Direct-to-S3 uploads eliminate the Go backend from the data path entirely, reducing memory footprint to a constant regardless of file size and removing the upload as a scaling bottleneck. Pre-signed URLs also let you enforce upload size limits at the SeaweedFS layer (via bucket policy or presigned URL expiration constraints) rather than in the Go process, which is more reliable and harder to bypass.

---

## Phase 3: Protocol & Schema Architecture

### Protobuf Schemas for Redpanda Events

**What:** Define Protobuf schemas for all Redpanda event types (`submission.created`, `build.completed`, `sandbox.deployed`, `bot.metrics`, `test.complete`) and integrate a schema registry (e.g., Redpanda Schema Registry) into the event pipeline. Producers serialize events to Protobuf before publishing; consumers deserialize with schema validation on receipt.

**Why:** The inter-service communication is already event-driven through Redpanda, and replacing it with gRPC streaming would be an architectural regression — losing replay, consumer groups, and independent scaling. What the platform actually needs is the serialization efficiency and type safety of Protobufs without abandoning the event-driven model. Protobufs are 3–10x smaller than equivalent JSON, reducing network transit time and memory allocation. Schema validation at the registry catches field mismatches, missing fields, and type coercion errors at build time or on first publish, eliminating the class of bugs caused by schema drift between publishers and consumers.

---

## Phase 4: Dedicated Compute & CPU Pinning

### Physically Separate Node Pools

**What:** Provision two distinct node pools in the cluster:

- **Platform Pool**: runs all infrastructure (Redpanda, TimescaleDB, Redis, SeaweedFS) and platform services (submission-api, build-service, sandbox-orchestrator, bot-orchestrator, telemetry-ingester, leaderboard-ws)
- **Sandbox Pool**: tainted to accept only sandbox pods (contestant matching engines), enforced via `NodeAffinity`

**Why:** Even with namespace-level isolation, shared node pools mean shared kernel scheduler, shared memory bandwidth, and shared network interfaces. Physical separation eliminates all cross-workload interference at the hardware level. This is the final step in ensuring that latency metrics are attributable solely to contestant code.

**Caveat:** Node pool isolation interacts directly with CPU pinning. Even with dedicated sandbox nodes, if two sandbox pods land on the same node and both are pinned, they share memory bandwidth and L3 cache. True isolation requires one sandbox pod per node, which means the node pool sizing directly determines concurrent test capacity. A `c5.2xlarge` with 8 vCPUs can host at most one 2-core sandbox pod with the static CPU manager, because the remaining 6 cores are reserved for system pods and aren't allocatable as pinned cores without careful kubelet configuration. This significantly affects the cost model for production.

### Static CPU Manager & Guaranteed QoS

**What:** Enable the Kubelet `static` CPU Manager policy on all sandbox pool nodes. Enforce the Guaranteed QoS class on all contestant pods by setting CPU requests equal to CPU limits (integer values only, e.g., `2` cores).

**Why:** The default `none` CPU Manager policy allows the kernel scheduler to time-slice cores across all pods on a node, introducing context-switch latency that contaminates p90/p99 measurements. The `static` policy pins containers to exclusive CPU cores — no other pod can schedule on those cores for the lifetime of the container. Combined with Guaranteed QoS, this gives each matching engine uninterrupted, dedicated CPU capacity. The result is latency metrics that reflect only the efficiency of the contestant's matching algorithm, not kernel scheduling decisions.

**Prerequisites:** The kubelet must be started with `--cpu-manager-policy=static` and the node must have integer CPU allocations available. On EKS managed node groups, this requires a custom launch template with a kubelet config file — document as a Terraform variable so it's not buried in instance type selection. The sandbox node group must use instance types with enough physical cores (`c5.2xlarge` with 8 vCPUs is a reasonable minimum for 2 pinned cores per contestant plus headroom for system pods). The static CPU manager only pins when containers request integer CPUs and the QoS class is Guaranteed; fractional requests silently fall back to the default scheduler. Admission control — either an OPA/Gatekeeper policy or a validating webhook — must enforce integer CPU requests on the `sandboxes` namespace. Without it, one misconfigured deployment breaks the fairness guarantee for all concurrent tests.

---

## Phase 5: Operational Maturity

### Rate Limiting & Team Authentication

**What:** Implement team identity verification (API keys, OAuth, or mutual TLS) and add per-team token-bucket rate limiting to `submission-api`. Key the bucket by verified team identity, not client IP.

**Status:** Deferred — team authentication is a prerequisite for meaningful rate limiting.

**Why:** Rate limiting without authentication would be keyed by client IP, which is useless behind NAT or a shared cluster node. This must be implemented before the platform is publicly accessible — without it, any team can flood the submission queue, starving other teams and filling disk with max-size uploads. Without authentication, there is no way to attribute submissions to teams or prevent impersonation.

### CI/CD Pipeline

**What:** Build a GitHub Actions (or equivalent) pipeline that runs `go build`, `errcheck`, `go vet`, unit tests, `helm lint`, and Protobuf/JSON schema validation for Redpanda event payloads on every push. Gate merges on all checks passing. Add a `just deploy` target that pushes images, runs `helm upgrade --install --wait`, and executes the smoke test.

**Why:** Currently every change requires manual build, install, and verification. A CI/CD pipeline catches regressions before they reach the cluster, enforces coding conventions automatically, and makes the dev → deploy cycle repeatable and auditable. Schema validation for event payloads catches an entire class of bugs that `go vet` cannot: wrong field names, missing fields, and type coercion errors between publishers and consumers. Several bugs in BUG.md are directly caused by this kind of schema drift.

### Alerting & Monitoring Dashboards

**What:** Deploy Prometheus + Grafana (or equivalent) with dashboards for: Kafka consumer lag per partition, build pod failure rate, sandbox pod restart count, TimescaleDB insert latency and hypertable size, Redis memory usage and pub/sub subscriber count, HTTP error rates per service, and HPA/KEDA scaling events. Configure alerts on SLO breaches (e.g., consumer lag > 1000, build failure rate > 5%, p99 latency > 2× baseline).

**Why:** The platform currently has no observability beyond `kubectl logs`. When something breaks, the only debugging path is manual pod inspection. Dashboards make degradation visible before it becomes an outage. Alerts ensure the operator is notified of failures rather than discovering them when teams report missing results. Monitoring is also a prerequisite for chaos testing — you cannot interpret chaos experiment results without dashboards.

### Integration & End-to-End Tests

**What:** Write tests that exercise the full submission lifecycle: upload a `.tar.gz` to `submission-api`, verify the Kafka event, confirm `build-service` compiles it, check `sandbox-orchestrator` deploys it, validate `bot-runner` connects and sends metrics, and assert `telemetry-ingester` writes to TimescaleDB and `leaderboard-ws` broadcasts the update.

**Why:** Unit tests verify individual functions but cannot catch integration failures: mismatched Kafka topics, wrong environment variable names, RBAC denials, network policy blocks, or schema drift. The platform's correctness depends on seven services cooperating across three namespaces. Without end-to-end tests, a breaking change in one service can silently break the entire pipeline.

### Load Testing Harness

**What:** Build a dedicated load testing tool that can spawn configurable numbers of bot pods (10, 50, 100, 500), each running a realistic submission → build → sandbox → metrics cycle. Collect aggregate p50/p90/p99 latency, error rates, and resource utilization across the full stack.

**Why:** The current bot-runner is designed for a single test run, not sustained load. Without a proper harness, there is no way to validate that the platform holds up under concurrent submissions, that KEDA scales correctly, that Redpanda handles partition throughput, or that TimescaleDB hypertable inserts don't become a bottleneck. Load testing is the only way to prove the platform meets its latency SLOs.

### Chaos Testing for Dependency Failures

**What:** Build automated chaos experiments that: kill Redpanda pods mid-submission, sever Redis connectivity during leaderboard updates, drop TimescaleDB write availability during metric ingestion, simulate SeaweedFS unreachability during artifact upload, and crash the bot-orchestrator mid-test after spawning bot runner Jobs but before publishing `test.complete`. Verify that each service handles the failure gracefully (retries, backoff, circuit break) and recovers without data loss.

**Why:** The platform assumes its dependencies are always available. In production, they won't be. Without chaos testing, there is no confidence that `context.WithTimeout` on `ProduceSync` actually prevents hung goroutines, that the telemetry ingester's flush loop drains correctly on reconnect, or that the leaderboard WebSocket re-subscribes after a Redis restart. The bot-orchestrator crash scenario is the most likely real-world failure mode: the sandbox pod leaks, the bot runner runs to completion and publishes metrics, but no one ever cleans up the sandbox (bug #183). Graceful degradation under partial failure is what separates a demo from a production system.

### Backup & Disaster Recovery Runbooks

**What:** Implement automated TimescaleDB backups (pg_dump or WAL-G) to object storage, Redis AOF persistence with periodic RDB snapshots, and Redpanda topic replication. Write runbooks for: restoring from backup, rebuilding a lost namespace, recovering from Kafka topic corruption, and rolling back a failed Helm upgrade.

**Why:** TimescaleDB holds all telemetry and scoring data. Redis holds the live leaderboard state. Redpanda holds in-flight submissions. None of these are backed up. A node failure, accidental `helm uninstall`, or corrupted hypertable means permanent data loss. Runbooks ensure recovery is a documented procedure, not an improvised crisis response.

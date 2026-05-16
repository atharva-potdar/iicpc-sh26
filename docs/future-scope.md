# Future Scope

## Phase 1: Event-Driven Autoscaling & Compute Isolation

### KEDA Kafka-Triggered Autoscaling

**What:** Replace CPU-based HPAs on `submission-api`, `build-service`, `sandbox-orchestrator`, and `telemetry-ingester` with KEDA `ScaledObjects` using Kafka topic lag triggers on `submission.lifecycle`.

**Why:** CPU-based scaling is a lagging indicator — replicas spin up after the load has already arrived. Kafka lag is a leading indicator: replicas scale in direct proportion to the actual backlog of submissions to process. `maxReplicaCount` must be hardcapped to the Redpanda partition count to avoid spawning idle consumers that add coordination overhead without increasing throughput.

### Node-Level Workload Isolation

**What:** Apply dedicated taints to sandbox nodes and configure `NodeAffinity` + tolerations so that platform namespace workloads (submission-api, build-service, Redpanda, TimescaleDB, Redis) are never scheduled on sandbox nodes, and vice versa.

**Why:** Platform workloads introduce noisy-neighbor effects that contaminate latency measurements. TimescaleDB compaction, Redpanda log flushes, and queue processing spikes all compete for CPU and I/O on shared nodes. Isolating sandbox pods onto dedicated nodes ensures that p50/p90/p99 latency metrics reflect only the contestant's code quality, not infrastructure contention.

---

## Phase 2: eBPF Networking & L7 Security Policies

### Cilium CNI Replacement

**What:** Replace the default CNI (kube-proxy/iptables) with Cilium, leveraging eBPF for pod-to-pod networking and service load balancing.

**Why:** iptables processes rules sequentially — each new rule adds linear latency to every packet traversal. Under a 50-bot load test, this overhead accumulates measurably in the ack latency path between bot fleet and matching engines. Cilium's eBPF datapath performs O(1) lookups, eliminating sequential rule evaluation and reducing tail latency variance.

### L7 Default-Deny Egress for Sandboxes

**What:** Deploy Cilium `CiliumNetworkPolicy` with default-deny egress on all sandbox pods. Whitelist only the required destinations: CoreDNS (UDP/TCP 53), Redpanda (TCP 9092), Kubernetes API (TCP 443), and SeaweedFS (TCP 8333). All other egress is denied at L7.

**Why:** The current NetworkPolicy-based egress rules operate at L3/L4 and cannot inspect application-layer protocols. An L7 policy prevents protocol smuggling (e.g., DNS tunneling over allowed ports) and ensures sandbox pods can only communicate with platform services using the intended protocols. This is a prerequisite for any production deployment handling untrusted contestant code.

---

## Phase 3: Protocol & Upload Architecture

### gRPC + Protobufs for Internal Communication

**What:** Replace REST/JSON with gRPC + Protobufs for all service-to-service communication (submission-api → build-service → sandbox-orchestrator → bot-orchestrator) and bot-to-engine WebSocket message framing.

**Why:** JSON parsing adds measurable overhead on both the serialization and deserialization paths. Protobufs are 3–10x smaller than equivalent JSON, reducing network transit time and memory allocation. gRPC enforces type safety at compile time, eliminating the class of bugs caused by mismatched field names or type coercion errors. The streaming nature of gRPC also replaces the current WebSocket text-frame protocol with a typed binary protocol for bot-to-engine communication.

### Pre-Signed S3 Uploads

**What:** Refactor `submission-api` `POST /submissions` to return a pre-signed SeaweedFS S3 upload URL instead of accepting the file through the Go backend. Clients upload `.tar.gz` files directly to SeaweedFS, then call a lightweight confirmation endpoint with the artifact key.

**Why:** The current flow routes the entire file through the Go process, which must hold the upload in memory (or spill to disk) before forwarding to SeaweedFS. Under concurrent load, this causes OOMKills and disk I/O bottlenecks. Direct-to-S3 uploads eliminate the Go backend from the data path entirely, reducing memory footprint to a constant regardless of file size and removing the upload as a scaling bottleneck.

---

## Phase 4: Dedicated Compute & CPU Pinning

### Physically Separate Node Pools

**What:** Provision two distinct node pools in the cluster:

- **Platform Pool**: runs all infrastructure (Redpanda, TimescaleDB, Redis, SeaweedFS) and platform services (submission-api, build-service, sandbox-orchestrator, bot-orchestrator, telemetry-ingester, leaderboard-ws)
- **Sandbox Pool**: tainted to accept only sandbox pods (contestant matching engines), enforced via `NodeAffinity`

**Why:** Even with namespace-level isolation, shared node pools mean shared kernel scheduler, shared memory bandwidth, and shared network interfaces. Physical separation eliminates all cross-workload interference at the hardware level. This is the final step in ensuring that latency metrics are attributable solely to contestant code.

### Static CPU Manager & Guaranteed QoS

**What:** Enable the Kubelet `static` CPU Manager policy on all sandbox pool nodes. Enforce the Guaranteed QoS class on all contestant pods by setting CPU requests equal to CPU limits (integer values only, e.g., `2` cores).

**Why:** The default `none` CPU Manager policy allows the kernel scheduler to time-slice cores across all pods on a node, introducing context-switch latency that contaminates p90/p99 measurements. The `static` policy pins containers to exclusive CPU cores — no other pod can schedule on those cores for the lifetime of the container. Combined with Guaranteed QoS, this gives each matching engine uninterrupted, dedicated CPU capacity. The result is latency metrics that reflect only the efficiency of the contestant's matching algorithm, not kernel scheduling decisions.

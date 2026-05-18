# Sample Design Document: "VoltStream" - Distributed Smart Grid Platform

**System Overview:** VoltStream ingests power usage telemetry from 50 million residential smart meters every 10 seconds. The system must process this stream to detect localized power outages, route data to billing, and handle massive network partitions gracefully.

## Section 1: Ingestion & RPC Resilience
* **Architectural Design:** We contrasted using an ephemeral message queue versus a persistent distributed log. Since telemetry data must fan out to real-time outage detection and batch billing systems, a distributed log (Kafka) is justified.
* **RPC Contracts & Backpressure:** If ingestion brokers slow down, edge gateways must return HTTP 429 rather than holding connections open, implementing backpressure to prevent cascade failures.
* **Idempotency & Retries:** Meters use exponential backoff and jitter. The ingestion endpoint enforces idempotency using `Meter_ID` and `Timestamp_Nonce` so duplicate transmissions don't result in double-billing.

## Section 2: Time, Ordering, & Stream Processing
* **Architectural Design:** Perfectly synchronized physical clocks are impossible across 50 million cheap meters. If a meter loses connection and dumps data later, wall-clock time is unsafe. We rely on logical watermarking for ordering.
* **Stream Processing & Checkpointing:** Aggregating power usage requires barrier-style checkpoints in the stream processor to ensure fault tolerance.
* **Delivery Semantics:** "Exactly-once effects" are achieved because at-least-once delivery from the log is acceptable since our downstream state machine is strictly idempotent.

## Section 3: Telemetry Storage & Partitioning
* **Architectural Design:** Range sharding by Timestamp creates massive write hotspots. Instead, we use a composite sharding strategy: hash sharding by `Meter_ID` combined with time-bucketing.
* **Quorums & Replication:** For this write-heavy workload, we use eventual consistency with a low write quorum (W=1, R=3) to prioritize availability over strong consistency.
* **Consensus for Storage Metadata:** Raft is required to maintain the shard map and elect leaders for specific partitions.

## Section 4: Transactions, Observability & Recovery
* **Architectural Design:** Moving data to the SQL billing ledger uses a choreography Saga. 2PC is too risky under partial failure for this cross-system transaction.
* **Observability:** We use metrics to identify drops in ingestion throughput, and distributed traces with context propagation to track a specific `Meter_ID`'s payload through the stream processor.
* **Global Snapshots & Recovery:** Backing up real-time grid state relies on the Chandy-Lamport distributed snapshot algorithm to capture a globally consistent snapshot without halting ingestion.

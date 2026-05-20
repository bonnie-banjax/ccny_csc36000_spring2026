# Project 2: Distributed File Sync & Storage

**Scenario:** The system must provide highly available object storage for petabytes of data, synchronize files across multiple client offline/online states, and resolve concurrent edits.

## System Picture
Use the following baseline mental model as a starting point for your design. You may change implementation details, but your architecture should still satisfy these core assumptions.

* **Main Actors:** User devices, sync API/service, metadata control plane, chunk or object storage layer, background repair and rebalance workers, conflict-resolution logic, observability systems.
* **Core Request Path:** A user edits or uploads a file on one device. The client sync engine uploads file chunks and metadata updates when online, or queues them locally when offline. The metadata layer commits a new file version and other devices later fetch the updated state. Background services repair replicas, rebalance data, and clean up stale or superseded versions.
* **Major Services:** Sync gateway, metadata database/router, version and permission metadata service, chunk storage nodes, placement/replication layer, conflict-resolution service, monitoring and alerting stack.
* **Scale Assumptions:** Petabytes of stored data, many concurrent uploads, devices that frequently disconnect and reconnect, multi-device synchronization, and occasional large tenants or shared folders that create localized hotspots.
* **Non-Negotiable Invariants:** Metadata must always point to the correct file version and chunk set. Permission changes must not violate security rules. Duplicate retries must not store duplicate chunks or corrupt manifests. Recovery must preserve durable user data and avoid inventing or losing committed file versions.

## Student 1: The Control Plane (Metadata Scaling & Sharding)
* **Architectural Design (0.75 to 1 page):** Design the relational database cluster tracking file metadata. Compare hash vs. range sharding and identify common hotspot causes.
* **Rebalancing Strategy (0.75 to 1 page):** Explain consistent hashing and virtual slots. Design a rebalancing strategy that limits user-visible disruption when adding new database nodes.
* **Isolation & Anomalies (0.75 to 1 page):** Discuss the anomalies allowed by snapshot isolation (like write skew) and why it matters for resolving concurrent changes to file permissions.

## Student 2: The Data Plane (Block Storage & Integrity)
* **Architectural Design (0.75 to 1 page):** Design the architecture that stores the actual file bytes. Explain the scaling limits of centralized metadata versus algorithmic data placement (like CRUSH).
* **Fault Tolerance & Storage (0.75 to 1 page):** Compare replication and erasure coding at a conceptual level. Understand the tradeoffs regarding availability and repair costs.
* **Quorums & Repair (0.75 to 1 page):** Describe quorum reads/writes (N, R, W) for fetching file chunks. Detail what R + W > N implies, and when read repair helps or hurts.

## Student 3: Sync Engine & Conflict Resolution (The Client Interface)
* **Architectural Design (0.75 to 1 page):** Design the protocol that syncs offline edits. Differentiate between physical time, logical time, and causal ordering.
* **Clocks & Conflict (0.75 to 1 page):** Explain why perfectly synchronized clocks are unrealistic. Use logical timestamps to reason about conflicts and stale reads when two offline users edit the same document.
* **Idempotency & Retries (0.75 to 1 page):** Implement safe retry patterns for the client sync engine. Give examples of how the "Upload Chunk" operation is made idempotent.

## Student 4: System Degradation & Chaos Testing (The SRE)
* **System Degradation (0.75 to 1 page):** Explain fault tolerance patterns like graceful degradation or bulkheads if a specific storage rack goes offline.
* **Chaos Engineering (0.75 to 1 page):** Write a brief runbook proposing a controlled failure experiment. What failure should you *not* chaos-test first, and why?
* **Observability (0.75 to 1 page):** Design telemetry required to monitor sync queues. Explain how backpressure shows up in the messaging system if the metadata database slows down.

# Project 1: Global Ride-Sharing Platform

**Scenario:** The system must handle millions of concurrent real-time GPS updates, accurately match riders to drivers without double-booking, and safely process payments across distributed services.

## System Picture
Use the following baseline mental model as a starting point for your design. You may change implementation details, but your architecture should still satisfy these core assumptions.

* **Main Actors:** Rider mobile app, driver mobile app, regional edge/API gateways, location-stream ingestion layer, matchmaking workers, trip service, payment service, notification service, analytics/observability systems.
* **Core Request Path:** Drivers continuously stream GPS pings to a regional edge service. Riders submit trip requests to the dispatch system. Matchmaking workers combine rider demand with the current active driver pool to reserve and assign a driver. Trip state is updated as the ride progresses, then payment is finalized after trip completion.
* **Major Services:** API gateway, distributed log or messaging backbone, active driver state store, dispatch shard workers, trip database, payment integration, tracing/metrics/logging stack.
* **Scale Assumptions:** Millions of concurrent driver pings, sub-second location freshness targets, geographically distributed deployment, multiple downstream consumers of location events, and regional hotspots such as airports or stadiums.
* **Non-Negotiable Invariants:** A driver must never be double-booked. Trip state transitions must remain valid and auditable. Duplicate client retries must not create duplicate assignments or duplicate charges. Recovery must not restore a state where billing and trip completion contradict each other.

## Student 1: Edge Ingestion & Streaming (The Location Firehose)
* **Architectural Design (0.75 to 1 page):** Design the ingestion pipeline for driver GPS pings. Contrast a traditional message queue with a distributed log. Detail how the data stream is partitioned (e.g., geohashing vs. driver ID) and the tradeoffs.
* **API & RPC Contracts (0.75 to 1 page):** Define the RPC contract for the mobile client sending pings. Explain how client deadlines and timeouts are configured.
* **Resilience & Overload (0.75 to 1 page):** Design a backpressure mechanism to prevent the API gateway from failing under sudden spikes. Detail the implementation of safe retry patterns (backoff + jitter) and circuit breakers.

## Student 2: Matchmaking Worker & Coordination (The Dispatcher)
* **Architectural Design (0.75 to 1 page):** Design the worker node system that pairs riders and drivers. Explain why consensus is needed for leader election among matchmaking workers to avoid double-booking. Evaluate building custom consensus vs. using ZooKeeper/etcd.
* **State & Concurrency (0.75 to 1 page):** Explain how distributed leases or fencing tokens prevent split-brain errors when assigning a driver.
* **Sharding & Hotspots (0.75 to 1 page):** Design the partitioning strategy for the active driver pool. Explain consistent hashing/slots, and discuss how you will handle geographic hotspots.

## Student 3: Trip State Machine & Distributed Transactions (The Ledger)
* **Architectural Design (0.75 to 1 page):** Design the database schema and state machine for a trip. Explain why distributed transactions are hard under partial failure.
* **Transaction Management (0.75 to 1 page):** Compare Two-Phase Commit (2PC) to the Saga pattern for handling final payment integration. Provide a sequence diagram of compensating transactions.
* **Concurrency Control (0.75 to 1 page):** Discuss how Multiversion Concurrency Control (MVCC) helps read performance when analytics engines query the trip database. Give examples of anomalies your isolation levels prevent.

## Student 4: Observability, Fault Tolerance, & Recovery (The Safety Net)
* **Distributed Tracing (0.75 to 1 page):** Design a tracing implementation with context propagation to track a trip from creation to billing. Explain how this helps diagnose tail latency.
* **Metrics & Dashboards (0.75 to 1 page):** Define the metrics required to detect a "retry storm". Discuss when to use logs versus traces.
* **Disaster Recovery & Snapshots (0.75 to 1 page):** Design the backup strategy. Explain how global snapshots work (using Chandy-Lamport concepts) and why consistency matters for checkpoints. Contrast recovery using only a log vs log + checkpoints.

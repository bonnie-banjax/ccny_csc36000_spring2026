# Project 3: Global Social Media Feed

**Scenario:** A microblogging platform facing immense read loads. It must accurately order global events, provide eventual consistency for feeds, and handle massive "hotspots" created by celebrity accounts.

## System Picture
Use the following baseline mental model as a starting point for your design. You may change implementation details, but your architecture should still satisfy these core assumptions.

* **Main Actors:** End users posting and reading tweets, API gateways, tweet-ingestion pipeline, timeline service, cache layer, search indexing service, social graph service, observability systems.
* **Core Request Path:** A user posts a tweet through a regional write API. The tweet is durably appended to an event pipeline, then consumed by downstream systems such as timeline fan-out, search indexing, and analytics. Readers fetch home timelines from a cache-heavy read path that depends on the social graph and previously materialized timeline fragments.
* **Major Services:** Write API, distributed log or streaming backbone, home-timeline service, cache cluster, search indexer, user-graph store, recommendation or ranking dependencies, tracing/metrics/logging stack.
* **Scale Assumptions:** Read-heavy traffic, global users posting from many regions, very high fan-out for celebrity accounts, eventual consistency in some user-visible paths, and sustained hotspot pressure on feeds, caches, and follower data.
* **Non-Negotiable Invariants:** A user's own tweets must appear in a sensible per-user order. Duplicate retries must not create duplicate tweets. The social graph must not diverge due to split-brain leadership. Recovery must not produce impossible feed, search, or follow states that contradict committed writes.

## Student 1: Feed Caching, Quorums & Eventual Consistency (The Read Path)
* **Architectural Design (0.75 to 1 page):** Design the timeline architecture. Contrast a fan-out-on-write model with a fan-out-on-read model. Discuss the tradeoffs of eventual consistency for the feed.
* **Quorums & Read Repair (0.75 to 1 page):** Design the distributed cache layer. Explain quorum configurations for the cache. Detail how read repair functions when a cache node returns stale data.
* **RPC & Performance (0.75 to 1 page):** Explain how tail latency hides user pain during feed loads, giving three causes. Design the RPC streaming model for pushing live updates.

## Student 2: Tweet Ingestion & Event Ordering (The Write Path)
* **Architectural Design (0.75 to 1 page):** Design the ingestion pipeline. Explain why assuming wall-clock order equals event order is unsafe for tweets originating globally. Design an ordering model using logical clocks.
* **Messaging & Streaming (0.75 to 1 page):** Compare using queues vs logs for passing tweets to the search indexing service. What ordering guarantees are practical at scale, and why does "exactly-once" become "exactly-once effects"?
* **Fault Tolerance (0.75 to 1 page):** Detail the implementation of safe retry patterns for the client posting a tweet. Give examples of how "Post Tweet" is made idempotent.

## Student 3: User Graph & Transactions (The Relationships)
* **Architectural Design (0.75 to 1 page):** Design the database storing "who follows whom." Compare hash vs range sharding and identify how a celebrity account creates a hotspot.
* **Transactions (0.75 to 1 page):** Design the transaction that occurs when a user follows someone. When should the application redesign to avoid a cross-shard transaction?
* **Consensus (0.75 to 1 page):** Explain the role of consensus (e.g., Raft) for maintaining cluster membership of the graph databases. Explain why leader election requires randomized timeouts.

## Student 4: Tracing & Global Recovery (The Overlooker)
* **Observability (0.75 to 1 page):** Design tracing spans necessary to debug a scenario where a tweet is successfully posted but fails to appear in search. Explain context propagation and why it's required.
* **Disaster Recovery (0.75 to 1 page):** Design a global snapshot algorithm for the graph database to ensure consistent backups without pausing the network. What makes a snapshot "consistent"?
* **Resilience (0.75 to 1 page):** Design a circuit breaker implementation for services relying on the graph database. Explain how the system will degrade gracefully if the "follow" service goes offline.

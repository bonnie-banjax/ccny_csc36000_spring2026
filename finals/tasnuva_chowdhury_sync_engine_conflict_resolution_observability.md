# Sync Engine, Conflict Resolution, and Observability

## 1. Sync Engine Architecture

The sync engine is the part of the system that runs on the user's device and keeps local files consistent with the cloud version. Since users may edit files from laptops, phones, tablets, or shared folders, the client cannot assume that the device is always online or that it has the newest version of every file. The main responsibility of the sync engine is to track local changes, upload them safely, download newer remote changes, and handle conflicts when two versions cannot be automatically merged.

When a user edits a file, the client first writes the change to local storage. This is important because the user should not lose work just because the device is offline, the app crashes, or the network becomes unstable. The client then records the change in a local sync queue. Each queued operation includes the file ID, parent folder ID, base version, operation type, chunk hashes, upload session ID, Hybrid Logical Clock timestamp, and idempotency key.

The base version is one of the most important fields. It represents the last committed version of the file that the client knew about before making the edit. This lets the server check whether the user edited the newest version or edited an older version while another device was also making changes. Without the base version, the server would have a much harder time detecting stale writes and could accidentally overwrite a committed version.

The local sync queue also helps the system handle offline-first behavior. If a user edits a file without internet access, the operation stays in the queue until the device reconnects. This creates a better user experience because the user can continue working even when the cloud is temporarily unreachable. The tradeoff is that the client becomes more complex. It must keep durable local state, retry failed uploads, detect when remote changes conflict with local changes, and avoid replaying the same operation incorrectly.

The upload path is split into two parts: chunk upload and metadata commit. First, the file is divided into chunks. Each chunk is identified by a content hash, such as SHA-256. The client uploads chunks through the sync gateway or storage API. Since the data plane uses a CRUSH-style algorithmic placement approach, the client does not decide which physical storage node receives each chunk. The storage layer maps the chunk to the correct placement group and replicas. This keeps the client protocol simpler because the client only tracks logical information such as chunk hashes, upload session IDs, and file metadata, while the storage system handles physical placement.

This is an intentional separation between the client interface and the storage layer. One alternative design would be to have the metadata service store and return the exact storage nodes for every chunk. That approach can work, but it creates a scaling bottleneck because every read or write may depend on centralized placement metadata. With CRUSH-style placement, the system can calculate placement algorithmically, which reduces centralized lookup pressure and helps the storage layer scale. The tradeoff is that the storage layer becomes more complex and must carefully handle rebalancing, degraded replicas, and placement group failures.

After the required chunks are uploaded, the client sends a metadata commit request to the sync service. The metadata commit contains the file ID, base version, new file version, chunk manifest, HLC timestamp, and idempotency key. The metadata layer is the source of truth for which version of a file is current. This separation protects the system from partially uploaded files. A file is not considered updated just because chunks were uploaded. It is only considered updated after the metadata commit succeeds.

This design prevents a major failure case: a client uploads only some chunks and then disconnects. If the metadata were updated too early, other devices might try to download a file version that points to missing chunks. By requiring the chunk upload to complete before the metadata commit, the system ensures that committed metadata always points to a complete chunk set. Background cleanup workers can later remove abandoned chunks from failed or incomplete upload sessions.

For downloads, the client periodically asks the sync service for changes after its last known sync token. The server responds with metadata changes such as new file versions, deleted files, permission updates, or conflict copies. The client compares the remote changes with its local state. If there are no local edits, the client downloads the new chunks and updates the file. If there are local edits based on an older version, the client treats this as a possible conflict.

The system uses Hybrid Logical Clocks, or HLCs, along with base-version checks to reason about ordering. An HLC combines physical time with a logical counter. The physical part keeps timestamps close to real time, while the logical counter helps preserve ordering when events happen close together or arrive out of order. This is easier to reason about than pure Lamport clocks because the timestamp still resembles wall-clock time. It is also more scalable than pure vector clocks because the system does not need to store a separate counter for every device or participant.

The biggest design tradeoff is between making syncing fast and making conflict detection correct. If the system simply accepted the newest timestamp, it would be faster and easier to build, but it could silently overwrite someone’s work. Instead, this design requires clients to send their base version and HLC metadata with each commit. This adds more metadata and more server-side checks, but it prevents lost updates and gives the system a clear way to detect concurrent edits.

### Main tradeoff for this section

The main tradeoff is simplicity versus correctness. A simple sync design could accept uploads in arrival order and let the latest upload win. That would reduce server-side checks, but it would be unsafe for offline edits. Our design is more complex because it keeps a local sync queue, uses chunk manifests, sends base versions, and separates chunk upload from metadata commit. The reason this is worth it is that file sync systems must protect user data first. A slower or more complicated sync process is acceptable, but silently losing a committed file version is not.

---

## 2. Clocks and Conflict Resolution

Perfectly synchronized clocks are unrealistic in a distributed file sync system. A laptop may be asleep, a phone may be offline, a user may manually change device time, or network delays may cause requests to arrive in a different order than they were created. Because of this, physical timestamps should not be used as the only method for deciding which edit is correct.

A timestamp can still be useful for display and debugging, but it should not be the only source of correctness. If the system uses only wall-clock time, then a device with a wrong clock could appear to have the “newer” edit even if that edit was based on an older file version. This is especially dangerous for offline edits because the upload time is not always the same as the edit time.

This design uses Hybrid Logical Clocks, or HLCs, to reason about file updates. An HLC combines physical time with a logical counter. The physical part keeps timestamps close to real time, while the logical counter helps preserve ordering when events happen close together or arrive out of order. This gives the system a practical timestamp that can still be sorted and understood, while avoiding full dependence on device wall-clock time.

There are several possible clock designs:

1. **Physical clocks only:**  
   This is the simplest approach because every device already has a wall-clock time. The problem is that physical clocks can drift, be changed manually, or be wrong after a device has been offline. This can cause stale edits to appear newer than committed edits.

2. **Lamport clocks:**  
   Lamport clocks provide logical ordering, but they do not capture real time. They can tell that one event happened after another in a logical sequence, but they are less natural for debugging and user-facing timestamps.

3. **Vector clocks:**  
   Vector clocks give more detailed causality information because they track counters for multiple participants. This makes them useful for detecting concurrent events. However, in a file sync system with many users, devices, shared folders, and tenants, vector clocks can become large and expensive. Every update may need to carry more metadata, and that metadata can grow as collaboration groups grow.

4. **Hybrid Logical Clocks:**  
   HLCs are a middle ground. They preserve a connection to physical time while adding a logical counter to handle ordering. They are not as precise as vector clocks for representing every causal relationship, but they are much more practical at large scale.

Our design chooses HLCs because the system needs something scalable, understandable, and good enough for ordering most events. However, HLCs are not used alone. The base-version check is still the main protection against lost updates.

For example, suppose two users share a document. User A opens version 10 on a laptop and then goes offline. User B opens the same version 10 on another device, stays online, and saves changes that become version 11. Later, User A comes back online and uploads their edit. If the system only uses physical time, User A’s later upload might overwrite User B’s version even though User A edited an outdated copy. This would violate the main requirement that the system must not lose committed file versions.

Instead, each file has a committed version maintained by the metadata service, and each update also carries an HLC timestamp. When a client edits a file, it includes the base version it edited from. The server compares the base version with the current committed version. If the base version still matches the current version, the commit can safely become the next version. If the current version has changed, then the system knows the client edited a stale version.

The HLC helps order events in a practical way, but the base-version check is still necessary for correctness. HLC timestamps can help the system compare updates, sort events, and debug ordering, but they should not be used to blindly choose a winner when two users edited the same old version offline. In that case, the edits are treated as concurrent from the file’s point of view, and the conflict-resolution service decides whether the changes can be merged safely.

The conflict-resolution service handles these cases based on file type and operation type. For simple metadata changes, such as renaming a file, the system may be able to merge changes automatically. For example, if one user renames a file and another user edits its contents, both changes can usually be preserved. However, if two users edit the same binary file while offline, the system may not be able to merge the contents safely. In that case, the system should preserve both versions. One version remains the main file, while the other becomes a conflict copy, such as `Project Report (Conflicted copy).docx`.

For collaborative documents, the system could use a more advanced merge strategy, such as operation-based merging or CRDT-style conflict handling. That would allow the system to merge fine-grained edits more intelligently. However, this adds significant complexity and may only work for supported document types. It is also harder to apply to arbitrary files like images, PDFs, videos, ZIP files, or binary project files.

For this system, the safer general design is version-based conflict detection with conflict copies when automatic merging is unsafe. This avoids silent data loss and keeps user data durable, even if the final conflict must be resolved manually.

### Main tradeoff for this section

The tradeoff is precision versus scalability. Vector clocks can describe causality in more detail, but they create too much metadata overhead in a system with many users, devices, and shared folders. HLCs are less heavy and easier to operate at scale, but they still need base-version checks to avoid silent overwrites. This gives the design a practical balance: HLC timestamps are useful for ordering and debugging, while base-version checks protect correctness.

### Potential falloffs

A possible weakness of this design is that it may still create conflict copies that users have to resolve manually. This is not ideal for user experience, especially in shared folders with frequent offline edits. However, the system chooses this because preserving both versions is safer than guessing incorrectly. Another weakness is that HLCs do not fully capture causality the way vector clocks do, but the metadata savings are important at large scale.

---

## 3. Idempotency and Safe Retries

Retries are necessary because partial failure is normal in distributed systems. A client might upload a chunk successfully, but the response could be lost because of a network timeout. From the client’s point of view, it does not know whether the operation failed or succeeded. If retries are not designed carefully, the client could upload duplicate chunks, create duplicate file versions, or corrupt the file manifest.

The sync engine uses idempotency to make retries safe. An operation is idempotent when running it multiple times has the same effect as running it once. This is important because clients should be allowed to retry without needing to know whether the previous request succeeded.

For the upload chunk operation, the client calculates a content hash for each chunk before upload. The hash becomes the chunk ID. If the same chunk is uploaded again, the storage layer can check whether that hash already exists. If it exists, the server returns success instead of storing another copy. This is useful for both retry safety and deduplication.

Because chunk placement is handled by a CRUSH-style algorithm in the storage layer, the client does not retry against specific storage nodes. It retries the logical upload operation using the same chunk hash, upload session ID, and idempotency key. The storage layer is responsible for mapping the chunk to the correct placement group and replicas. This means retry behavior stays stable even if the physical storage nodes change due to rebalancing or repair.

An example upload chunk request would include:

- file ID
- chunk index
- chunk hash
- chunk size
- upload session ID
- idempotency key
- raw chunk bytes

The server verifies that the hash of the received bytes matches the chunk hash sent by the client. If the hash does not match, the server rejects the chunk because the data may have been corrupted in transit. If the chunk already exists, the server can return success without writing a duplicate. If the chunk does not exist, the server stores it and records it as part of the upload session.

There are two common failure cases this design protects against. The first is a lost response. The chunk may have been stored successfully, but the client never receives the success message. When the client retries, the server sees that the chunk hash already exists and returns success. The second is a duplicate request. If the client accidentally sends the same chunk twice, the content hash prevents duplicate storage.

The metadata commit also needs to be idempotent. The client sends a unique idempotency key with the commit request. If the client retries the same commit because of a timeout, the metadata service checks whether that idempotency key was already processed. If it was already processed, the server returns the original result instead of creating another file version. This prevents duplicate versions from being created by repeated retry attempts.

This is especially important because metadata commits change the visible state of the file. A duplicate chunk is wasteful, but a duplicate metadata commit can be more serious. It could make the system believe that the same edit created multiple versions. Idempotency keys prevent this by making the commit operation safe to retry.

The client also uses exponential backoff when retrying failed requests. Retrying immediately in a tight loop can overload the sync gateway, metadata service, or storage layer. With exponential backoff, the client waits longer after each failure. The client should also add jitter so that many devices do not retry at exactly the same time after a temporary outage.

There is also a difference between retryable and non-retryable errors. A timeout, temporary storage overload, or `503 Service Unavailable` response should usually be retried. A permission denied error, invalid chunk hash, or conflict error should not be retried blindly because the request itself needs user action or client correction. Separating retryable errors from permanent errors prevents the client from creating unnecessary load.

### Main tradeoff for this section

The main tradeoff is extra bookkeeping versus retry safety. Idempotency requires the system to store upload session IDs, idempotency keys, chunk hashes, and sometimes previous responses. This adds storage and cleanup complexity. However, the alternative is much worse. Without idempotency, normal network timeouts could create duplicate chunks, duplicate file versions, or corrupted manifests. Since partial failure is expected in distributed systems, idempotency is necessary for correctness.

### Potential falloffs

One possible issue is that idempotency keys cannot be stored forever. The system needs a retention window and cleanup process. If a client retries after the idempotency record expires, the server may not recognize the operation. To reduce this risk, clients should retry within a reasonable time window, and abandoned upload sessions should be cleaned up carefully. Another issue is hash collision risk, but using a strong hash such as SHA-256 makes this extremely unlikely in practice.

---

## 4. Observability for Sync Queues and Backpressure

Observability is needed because failures in a distributed file sync system are not always obvious. A user may only notice that files are “stuck syncing,” but the real cause could be a slow metadata database, overloaded chunk storage nodes, unavailable placement groups, a backed-up message queue, or a bad deployment. The system needs metrics, logs, and traces that show where requests are slowing down and whether data is still moving safely through the system.

The most important metrics are sync queue depth, sync delay, upload success rate, retry rate, metadata commit latency, chunk upload latency, conflict rate, and error rate. Sync queue depth shows how many operations are waiting to be processed. Sync delay shows how long an operation waits before being completed. If the queue depth and delay are both increasing, that usually means the system is receiving work faster than it can process it.

The system should monitor the sync queue at multiple points. On the client side, the queue shows how many local operations are waiting to upload. On the server side, queue depth and consumer lag show whether background workers are keeping up with metadata commits, conflict resolution, notifications, and repair tasks. Looking at only one queue is not enough because the bottleneck may be on the client, sync gateway, metadata service, storage layer, or background worker pool.

Since the data plane uses CRUSH-style placement, observability should also include storage placement and repair metrics. The system should track unavailable placement groups, degraded replicas, chunk repair backlog, rebalancing progress, and failed chunk writes. These metrics help engineers tell the difference between a normal sync delay and a deeper storage-layer problem.

The system should also track client-side metrics. For example, the client should report how many files are waiting to upload, how many retries happened, and how long the device has been behind the latest server version. This matters because a server may look healthy while some users are still stuck due to local network issues, large files, permission conflicts, or an old client version.

Logs should include structured fields such as user ID, device ID, file ID, upload session ID, request ID, idempotency key, base version, committed version, HLC timestamp, chunk hash, and error type. These logs help engineers debug specific sync problems without guessing. For example, if a user says a file disappeared, engineers should be able to trace whether the file was deleted, moved, conflicted, blocked by permissions, or delayed because chunk storage was degraded.

Distributed tracing is also important. A single sync operation may pass through the sync gateway, metadata service, chunk storage service, conflict-resolution service, and background workers. Each request should carry a trace ID so we can see the full path of the operation. This makes it easier to find whether latency is coming from the client upload, the metadata commit, the storage layer, or a downstream queue.

Backpressure happens when one part of the system slows down and causes work to build up behind it. If the metadata database slows down, the sync gateway may still accept uploads, but metadata commits will take longer. This can cause the commit queue to grow, retry rates to increase, and clients to see files stuck in a pending state. The messaging system may show backpressure through growing consumer lag, increasing queue depth, longer message age, and higher timeout rates.

Backpressure can also happen if storage placement or repair slows down. For example, if a rack failure causes many chunks to become degraded, the storage system may spend resources repairing replicas. This can increase chunk write latency and make uploads slower. Observability should show whether sync delay is coming from metadata commits, chunk writes, placement group degradation, or background repair work.

The system should respond to backpressure by slowing down safely instead of failing randomly. For example, the sync gateway can temporarily limit large uploads, reduce background repair priority, or return retryable errors with a `Retry-After` value. The client can keep local changes safely queued and retry later. This is graceful degradation because the user’s local work is preserved even if cloud sync is delayed.

Good alerting should focus on user impact, not just machine health. For example, high CPU usage on one service may not matter if sync is still healthy. However, increasing sync delay, high retry rates, growing queue age, or a spike in failed metadata commits directly affects users. Alerts should be tied to symptoms such as “files are taking too long to sync” or “metadata commits are timing out,” not only infrastructure-level signals.

### Main tradeoff for this section

The main tradeoff in observability is visibility versus cost. Collecting every metric, log, and trace can become expensive at petabyte scale. However, collecting too little makes it difficult to debug failures. A balanced approach is to collect high-level metrics for every operation, structured logs for important events, and sampled traces for detailed request paths. Errors, retries, conflicts, metadata commits, and storage degradation events should receive more detailed logging because they are the most useful during incidents.

### Potential falloffs

One risk is alert fatigue. If the system alerts on every small delay or temporary retry spike, engineers may start ignoring alerts. To avoid this, alerts should be based on sustained user impact, such as sync delay staying high for several minutes or retry rates exceeding a normal threshold. Another risk is too much telemetry volume. At large scale, logs and traces can become expensive quickly. Sampling and retention policies are needed so the system keeps enough information for debugging without overwhelming storage and monitoring systems.

---

## Presentation Summary

My section focuses on the client sync engine, conflict resolution, safe retries, and observability. The main design choice is that the system should not blindly trust physical timestamps or accept the latest upload as the winner. Users often edit files offline, and requests can arrive late or out of order. To avoid losing committed file versions, the system uses base-version checks and Hybrid Logical Clocks. HLCs give practical ordering without the metadata overhead of full vector clocks, while base-version checks protect correctness.
 
For storage interaction, the client uploads chunks through a logical storage API. Since the data plane uses CRUSH-style placement, the client does not need to know which physical storage nodes hold each chunk. This keeps the sync protocol simpler while allowing the storage layer to scale and rebalance independently.

For retries, the system uses idempotency keys and content hashes. This allows the client to safely retry chunk uploads and metadata commits after timeouts without creating duplicate chunks or duplicate file versions.

For observability, the system monitors sync queue depth, sync delay, retry rates, metadata commit latency, chunk upload latency, conflict rate, storage degradation, and backpressure. The goal is not only to know when a server is unhealthy, but to know when users are actually experiencing delayed or stuck sync. If part of the system slows down, the system should degrade safely by preserving local edits and retrying later instead of losing data.
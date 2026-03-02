# Raft + Gateway Direct Messaging Group Project (Single Machine, Port-Based)

## Summary
Your team will build a **Raft replicated log** (one Raft cluster) and a **simple gRPC Gateway** that clients use for **1:1 direct messaging**.  
Everything must run on **one machine** using **host:port** addresses (default `127.0.0.1`), while remaining configurable to move processes to other hosts later.

**Storage is in-memory only.** (No databases, no disk persistence.)

This repository contains:
- `README.md` (this file): the project requirements and run contracts
- `tests/`: an automated test suite that validates correctness

---

## Team size
Groups of **3–4 members**.

---

## Final deliverables
You must implement:

1. **Raft Replica Node** (run **5** instances by default)  
2. **Gateway** (single instance)  
3. **Scripts** required by the test suite (see below)

---

## Non-goals (keep it simple)
- No authentication
- No encryption/TLS
- No sharding/partitioning
- No chat rooms; only **direct messages** between two users

---

## Required behavior (what your system must do)

### A) Raft correctness (high level)
Your replica cluster must implement the Raft algorithm described in the prompt, including:
- Roles: **Follower, Candidate, Leader**
- Terms (monotonic), stepping down on higher term
- Leader election via **RequestVote** with “up-to-date log” voting rule
- Heartbeats (AppendEntries with no entries)
- Log replication with **AppendEntries**
- Commit after **majority** acknowledgments
- Followers reject AppendEntries if `prev_log_index/prev_log_term` mismatch
- Leader repairs follower logs by finding a common prefix and overwriting the follower’s uncommitted suffix

### B) Application semantics (direct messaging)
- A conversation is defined by two user ids: `user_a = min(u1,u2)`, `user_b = max(u1,u2)`
- Writes are ordered via the Raft log; reads must reflect committed/applied state
- Client retries must not create duplicates:
  - Each send includes `(client_id, client_msg_id)`
  - Your state machine must deduplicate on this pair

### C) Failure + recovery (in-memory)
Your system must tolerate and recover from:
- **Leader failure** (crash): a new leader is elected; the system continues
- **Follower failure** (crash): the system continues (as long as quorum remains)
- **Candidate failure** (crash during election): the system still converges to a leader

**Recovery requirement:**  
If any replica is crashed and then restarted, it must **catch up** (via Raft log repair from the leader) until it can safely serve reads again.

### D) Read responses must indicate which node(s) served the read
The Gateway's read RPC response must return:
- the message history, and
- a list of replica addresses used to satisfy the read (`served_by`).

---

## Required network topology (single machine, ports)
Default expected ports:

- Gateway: `127.0.0.1:50051`
- Replicas (5):
  - `127.0.0.1:50061`
  - `127.0.0.1:50062`
  - `127.0.0.1:50063`
  - `127.0.0.1:50064`
  - `127.0.0.1:50065`

All addresses must remain configurable as `host:port`.

---

## Required gRPC APIs (contract used by tests)
You must implement the gRPC services exactly as defined in:
- `tests/protos/direct_gateway.proto` (Gateway API)
- `tests/protos/replica_admin.proto` (Replica Status API used by tests)

Your Gateway must implement `direct.DirectGateway`.  
Each replica must implement `replica.ReplicaAdmin`.

> The tests compile these `.proto` files to create clients.

---

## Required scripts (used by tests)
Create these scripts in `scripts/` at the repository root:

1) `scripts/run_cluster.sh`
- Starts **5 replicas** and **1 gateway** in the background
- Creates/overwrites `.runtime/cluster.json` with PIDs and addresses (schema below)
- Must return exit code 0 on success

2) `scripts/stop_cluster.sh`
- Stops all processes started by `run_cluster.sh` (best-effort)
- Must return exit code 0 even if some processes are already stopped

3) `scripts/start_replica.sh <replica_id>`
- Starts a single replica `<replica_id>` (1–5) and updates `.runtime/cluster.json` PID for that replica

4) `scripts/stop_replica.sh <replica_id>`
- Stops a single replica `<replica_id>` and updates `.runtime/cluster.json` PID to `null` for that replica

### `.runtime/cluster.json` schema (required)
`run_cluster.sh` must write:

```json
{
  "gateway": {"addr": "127.0.0.1:50051", "pid": 12345},
  "replicas": [
    {"id": 1, "addr": "127.0.0.1:50061", "pid": 12346},
    {"id": 2, "addr": "127.0.0.1:50062", "pid": 12347},
    {"id": 3, "addr": "127.0.0.1:50063", "pid": 12348},
    {"id": 4, "addr": "127.0.0.1:50064", "pid": 12349},
    {"id": 5, "addr": "127.0.0.1:50065", "pid": 12350}
  ]
}
```

- `pid` may be `null` if stopped
- Addresses must match what the processes are actually listening on

---

---

## Manual demo (run two clients and exchange messages)

This section is for a quick **human demo** that direct messages can be written and read through the Gateway.

### 1) Start the cluster (Gateway + replicas)
From your repo root:

```bash
bash scripts/run_cluster.sh
```

By default, the Gateway should listen on `127.0.0.1:50051` and replicas on `127.0.0.1:50061-50065`.

### 2) Install client deps + generate gRPC stubs for the client
The provided `direct_client.py` imports `direct_pb2` and `direct_pb2_grpc`, so you must generate them from `direct.proto` first.

```bash
python -m pip install grpcio grpcio-tools protobuf
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. direct.proto
```

You should now have:
- `direct_pb2.py`
- `direct_pb2_grpc.py`

### 3) Run two clients (two terminals)

**Terminal A (Alice → Bob):**
```bash
python direct_client.py --gateway 127.0.0.1:50051 --me alice --peer bob
```

**Terminal B (Bob → Alice):**
```bash
python direct_client.py --gateway 127.0.0.1:50051 --me bob --peer alice
```

### Expected outcome
Each client prints a short banner and either history or “live”, then waits for input:

- You should see lines similar to:
  - `[client] --- live ---`
  - `[client] type messages + Enter. Commands: /help, /quit`

Now type a message in **Terminal A** and press Enter:

- In **Terminal A**, you should see the message show up like:
  - `[1] me: hello bob`
- In **Terminal B**, you should see:
  - `[1] alice: hello bob`

Then reply in **Terminal B**:

- Terminal B:
  - `[2] me: hi alice`
- Terminal A:
  - `[2] bob: hi alice`

### Notes / common gotchas
- The client displays messages from the **SubscribeConversation server-stream**. If sending works but you never see messages appear, your Gateway likely hasn’t implemented `SubscribeConversation` or isn’t streaming committed events to subscribers.
- To validate “read” manually: stop a client and start it again. On startup it calls `GetConversationHistory` and prints the last N events as history.
- Exit with `/quit`.

## Running the tests

### Install test dependencies
Create and activate a venv, then:

```bash
pip install -r requirements-test.txt
```

### Run
From repo root:

```bash
pytest -q
```

The tests will:
- call `scripts/run_cluster.sh`
- run gRPC calls against the Gateway and replicas
- crash/restart replicas via scripts
- validate ordering, elections, failure handling, and recovery catch-up

---

## What the tests validate (minimum)
- Messages are returned **in the order sent** (by `seq` and by content)
- Leader election follows Raft rules (including **up-to-date log** voting)
- Candidate / leader / follower crash scenarios converge correctly
- After restart, a replica catches up and can serve reads again
- Reads return `served_by` showing which replica(s) were used

---

## Academic honesty
All team members must contribute. Use Git commits and (optionally) a short `CONTRIBUTIONS.md` to document roles.

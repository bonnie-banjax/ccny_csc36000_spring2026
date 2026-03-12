from __future__ import annotations

import sys
import os
import time
import json
import grpc
import random
import threading
import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent import futures

# This makes sure Python can find the generated protobuf files.
sys.path.append(os.path.join(os.path.dirname(__file__), "generated"))

import replica_admin_pb2
import replica_admin_pb2_grpc


# Raft roles
FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"

# The scripts create .runtime/cluster.json
RUNTIME_DIR = os.path.join(os.path.dirname(__file__), ".runtime")
CLUSTER_JSON = os.path.join(RUNTIME_DIR, "cluster.json")


def ordered_users(u1: str, u2: str):
    # Normalizing the conversation (user_a = min(u1,u2), user_b = max(u1,u2)) here.
    return (u1, u2) if u1 <= u2 else (u2, u1)


def now_ms():
    #Current time in milliseconds for message timestamps.
    return int(time.time() * 1000)


def parse_addr(addr: str):
    # Split 'host:port' into ('host', port).
    host, port = addr.rsplit(":", 1)
    return host, int(port)


def internal_addr(addr: str):
    # Using a second port for the replica's internal traffic.
    # Example:
    #  gRPC status API: 127.0.0.1:50061
    #  internal Raft/gateway helper API: 127.0.0.1:51061
    host, port = parse_addr(addr)
    return host, port + 1000


def post_json(addr: str, path: str, payload: dict, timeout: float = 1.0):
    # Small helper for internal HTTP+JSON calls between replicas.
    # replica_admin.proto only exposes Status(), so you'd still need a way for replicas to # exchange Raft messages.
    host, port = internal_addr(addr)
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        body = json.dumps(payload).encode("utf-8")
        conn.request("POST", path, body=body, headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        raw = resp.read()
        if resp.status != 200:
            raise RuntimeError(f"{path} failed with status {resp.status}")
        return json.loads(raw.decode("utf-8")) if raw else {}
    finally:
        conn.close()


def load_replica_addrs(default_host: str):
    # Load replica addresses from .runtime/cluster.json if it exists.
    # If not, fall back to the default addresses.
    if os.path.exists(CLUSTER_JSON):
        try:
            with open(CLUSTER_JSON, "r") as f:
                data = json.load(f)
            addrs = [str(r["addr"]) for r in data.get("replicas", []) if "addr" in r]
            if addrs:
                return addrs
        except Exception:
            pass

    # Default 5-node cluster layout
    return [f"{default_host}:{50060 + i}" for i in range(1, 6)]


class ReplicaAdmin(replica_admin_pb2_grpc.ReplicaAdminServicer):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.addr = f"{host}:{port}"

        # The scripts do not really pass replica id directly, so it was derived from the port
        # 50061 -> 1, 50062 -> 2, etc.
        self.node_id = port - 50060

        # Figure out who the other replicas are.
        self.all_replicas = load_replica_addrs(host)
        self.peer_addrs = [a for a in self.all_replicas if a != self.addr]

        # Lock because multiple threads will touch shared Raft state.
        self.lock = threading.RLock()
        self.stop_event = threading.Event()

        # -----------------------------
        # Basic Raft state
        # -----------------------------
        self.role = FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.leader_hint = ""
        self.log = []

        # commit_index = highest committed log entry
        # last_applied = highest log entry already applied to the chat state
        self.commit_index = 0
        self.last_applied = 0

        # These are leader-only tracking structures used to replicate logs.
        self.next_index = {}
        self.match_index = {}

        # -----------------------------
        # State machine / chat data
        # -----------------------------
        # messages[(user_a, user_b)] = list of committed events
        self.messages = {}

        # seen_messages[(client_id, client_msg_id)] = seq
        # This is for deduplication
        self.seen_messages = {}

        # -----------------------------
        # Timing
        # -----------------------------
        self.heartbeat_interval = 0.20
        self.last_heartbeat_sent = 0.0
        self.last_heartbeat = time.time()
        self.election_deadline = 0.0
        self.reset_election_deadline()

        # Background thread that handles elections and heartbeats.
        self.timer_thread = threading.Thread(target=self._run_timer, daemon=True)
        self.timer_thread.start()

    # -------------------------------------------------
    # Required gRPC API used by the tests
    # -------------------------------------------------
    def Status(self, request, context):
        # The tests call this directly to inspect each replica.
        # So these values need to actually reflect the node's real state.
        with self.lock:
            last_idx = len(self.log)
            last_term = self.log[-1]["term"] if self.log else 0
            # 1. Capture types before the return
            raw_val = self.role
            raw_type = type(raw_val).__name__

            # 2. Get the integer value the way you currently are
            try:
                mapped_int = replica_admin_pb2.Role.Value(raw_val)
                mapped_type = type(mapped_int).__name__
            except Exception as e:
                mapped_int = f"CRASH: {e}"
                mapped_type = "None"

            # 3. Write to stderr with immediate flush
            sys.stderr.write(
                f"[DEBUG Node {self.node_id}] "
                f"RoleString: '{raw_val}' ({raw_type}) -> "
                f"ProtoInt: {mapped_int} ({mapped_type})\n"
            )
            sys.stderr.flush()

            # This is the return the test depends on
            return replica_admin_pb2.StatusResponse(
                id=self.node_id,
                role=mapped_int, # MUST be an int for the test's 's.role == 2' check
                term=self.current_term,
                leader_hint=self.leader_hint,
                last_log_index=len(self.log),
                last_log_term=self.log[-1]["term"] if self.log else 0,
                commit_index=self.commit_index,
            )
            # return replica_admin_pb2.StatusResponse(
            #     id=self.node_id,
            #     role=replica_admin_pb2.Role.Value(self.role),
            #     term=self.current_term,
            #     leader_hint=self.leader_hint,
            #     last_log_index=last_idx,
            #     last_log_term=last_term,
            #     commit_index=self.commit_index,
            # )

    # -------------------------------------------------
    # Timer logic
    # -------------------------------------------------
    def _run_timer(self):
        # One background loop handles: election timeout for followers/candidates and heartbeat sending for leaders
        while not self.stop_event.is_set():
            try:
                should_start_election = False
                should_send_heartbeats = False

                with self.lock:
                    now = time.time()

                    if self.role == LEADER:
                        # Leaders periodically send AppendEntries even if empty.
                        if now - self.last_heartbeat_sent >= self.heartbeat_interval:
                            self.last_heartbeat_sent = now
                            should_send_heartbeats = True
                    else:
                        # Followers/candidates start an election if they timeout.
                        if now >= self.election_deadline:
                            should_start_election = True

                if should_start_election:
                    self._start_election()

                if should_send_heartbeats:
                    for peer in list(self.peer_addrs):
                        threading.Thread(
                            target=self._send_append_entries_to_peer,
                            args=(peer,),
                            daemon=True,
                        ).start()

                time.sleep(0.05)
            except Exception:
                # Timer doesn't die from just one bad iteration.
                time.sleep(0.1)

    def reset_election_deadline(self):
        # Randomized timeout helps avoid split votes.
        self.election_deadline = time.time() + random.uniform(1.0, 1.8)

    # -------------------------------------------------
    # Small helpers for reading log state
    # -------------------------------------------------
    def _last_log_index(self):
        return len(self.log)

    def _last_log_term(self):
        return self.log[-1]["term"] if self.log else 0

    def _get_entry(self, index):
        # Raft log index is 1-based, but Python lists are 0-based.
        # So entry 1 is stored at self.log[0].
        if index <= 0:
            return None
        pos = index - 1
        if 0 <= pos < len(self.log):
            return self.log[pos]
        return None

    # -------------------------------------------------
    # Role transitions
    # -------------------------------------------------
    def _become_follower(self, term, leader_hint=""):
        # Step down to follower when seeing a higher term or valid leader.
        self.role = FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.leader_hint = leader_hint
        self.last_heartbeat = time.time()
        self.reset_election_deadline()

    def _become_leader(self):
        # When this node wins the election, initialize leader replication state.
        self.role = LEADER
        self.leader_hint = self.addr

        # next_index starts at leader's last log index + 1 for every follower.
        next_idx = self._last_log_index() + 1
        self.next_index = {peer: next_idx for peer in self.peer_addrs}

        # match_index is how far each follower is known to be replicated.
        self.match_index = {peer: 0 for peer in self.peer_addrs}

        self.last_heartbeat_sent = 0.0

    def _candidate_is_up_to_date(self, cand_last_index, cand_last_term):
        # Voting rule from Raft:
        # grant vote only if candidate log is at least as up to date as mine.
        my_last_term = self._last_log_term()
        my_last_index = self._last_log_index()

        if cand_last_term != my_last_term:
            return cand_last_term > my_last_term
        return cand_last_index >= my_last_index

    # -------------------------------------------------
    # Apply committed entries to chat state
    # -------------------------------------------------

    def _apply_committed_entries(self):
        # Only committed entries should affect reads.
        # This is where the Raft log turns into actual message history.
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self._get_entry(self.last_applied)
            if entry is None:
                break

            dedup_key = (entry["client_id"], entry["client_msg_id"])

            # If this exact client request was already applied, skip it.
            if dedup_key in self.seen_messages:
                continue

            conv_key = (entry["user_a"], entry["user_b"])
            event = {
                "seq": entry["index"],
                "from_user": entry["from_user"],
                "to_user": entry["to_user"],
                "user_a": entry["user_a"],
                "user_b": entry["user_b"],
                "text": entry["text"],
                "server_time_ms": entry["server_time_ms"],
                "client_id": entry["client_id"],
                "client_msg_id": entry["client_msg_id"],
            }

            self.messages.setdefault(conv_key, []).append(event)
            self.seen_messages[dedup_key] = entry["index"]

    def _maybe_advance_commit(self):
        # Leader advances commit_index when a majority has replicated an entry.
        if self.role != LEADER:
            return

        # Count leader itself plus follower match_index values.
        replicated = [self._last_log_index()] + list(self.match_index.values())
        replicated.sort(reverse=True)

        # In a 5-node cluster, the majority point is the 3rd highest value.
        quorum_pos = len(replicated) // 2
        candidate_commit = replicated[quorum_pos]

        if candidate_commit <= self.commit_index:
            return

        entry = self._get_entry(candidate_commit)
        if entry is None:
            return

        # Standard safety rule: only advance by counting entries from current term.
        if entry["term"] != self.current_term:
            return

        self.commit_index = candidate_commit
        self._apply_committed_entries()

    # -------------------------------------------------
    # Election logic
    # -------------------------------------------------
    def _start_election(self):
        # Candidate increments term, votes for itself, and asks peers for votes.
        with self.lock:
            if self.role == LEADER:
                return

            self.role = CANDIDATE
            self.current_term += 1
            term_started = self.current_term
            self.voted_for = self.node_id
            self.leader_hint = ""
            self.last_heartbeat = time.time()
            self.reset_election_deadline()

            votes = 1
            last_idx = self._last_log_index()
            last_term = self._last_log_term()

        votes_lock = threading.Lock()

        def ask_peer(peer):
            nonlocal votes
            payload = {
                "term": term_started,
                "candidate_id": self.node_id,
                "candidate_addr": self.addr,
                "last_log_index": last_idx,
                "last_log_term": last_term,
            }
            try:
                resp = post_json(peer, "/request_vote", payload, timeout=1.0)
            except Exception:
                return

            with self.lock:
                their_term = int(resp.get("term", 0))
                if their_term > self.current_term:
                    self._become_follower(their_term, "")
                    return

            if bool(resp.get("vote_granted")):
                with votes_lock:
                    votes += 1

        threads = []
        for peer in list(self.peer_addrs):
            t = threading.Thread(target=ask_peer, args=(peer,), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=1.2)

        with self.lock:
            if self.role != CANDIDATE or self.current_term != term_started:
                return

            # Majority in a 5-node cluster is 3.
            if votes >= 3:
                self._become_leader()

    def handle_request_vote(self, data):
        # Internal RPC used by other replicas during election.
        with self.lock:
            term = int(data["term"])
            candidate_id = int(data["candidate_id"])
            cand_last_index = int(data["last_log_index"])
            cand_last_term = int(data["last_log_term"])

            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}

            if term > self.current_term:
                self._become_follower(term, "")

            can_vote = self.voted_for is None or self.voted_for == candidate_id
            up_to_date = self._candidate_is_up_to_date(cand_last_index, cand_last_term)

            grant = can_vote and up_to_date
            if grant:
                self.voted_for = candidate_id
                self.last_heartbeat = time.time()
                self.reset_election_deadline()

            return {"term": self.current_term, "vote_granted": grant}

    # -------------------------------------------------
    # AppendEntries logic
    # -------------------------------------------------
    def handle_append_entries(self, data):
        # Internal RPC used for: empty heartbeats and real log replication
        with self.lock:
            term = int(data["term"])
            leader_addr = str(data["leader_addr"])
            prev_log_index = int(data["prev_log_index"])
            prev_log_term = int(data["prev_log_term"])
            entries = list(data.get("entries", []))
            leader_commit = int(data["leader_commit"])

            if term < self.current_term:
                return {
                    "term": self.current_term,
                    "success": False,
                    "match_index": self._last_log_index(),
                }

            # If valid leader is contacting me, I should be follower.
            if term > self.current_term or self.role != FOLLOWER:
                self._become_follower(term, leader_addr)

            self.leader_hint = leader_addr
            self.last_heartbeat = time.time()
            self.reset_election_deadline()

            # Prefix check: follower rejects append if prefix does not match.
            if prev_log_index > 0:
                prev_entry = self._get_entry(prev_log_index)
                if prev_entry is None or prev_entry["term"] != prev_log_term:
                    return {
                        "term": self.current_term,
                        "success": False,
                        "match_index": self._last_log_index(),
                    }

            # Append entries. If follower has a conflicting suffix, delete it.
            insert_after = prev_log_index
            for incoming in entries:
                existing = self._get_entry(insert_after + 1)

                if existing is not None and existing["term"] != incoming["term"]:
                    # Keep only the common prefix.
                    self.log = self.log[:insert_after]
                    existing = None

                if existing is None:
                    self.log.append(incoming)

                insert_after += 1

            # Update commit index from leader, but only up to my own last log index.
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self._last_log_index())
                self._apply_committed_entries()

            return {
                "term": self.current_term,
                "success": True,
                "match_index": self._last_log_index(),
            }

    # -------------------------------------------------
    # Leader-side replication
    # -------------------------------------------------
    def _send_append_entries_to_peer(self, peer):
        # Leader tries to bring follower in sync.
        # If follower rejects, next_index is backed up to find the common prefix.
        with self.lock:
            if self.role != LEADER:
                return False

            next_idx = self.next_index.get(peer, self._last_log_index() + 1)
            prev_idx = next_idx - 1
            prev_term = 0

            if prev_idx > 0:
                prev_entry = self._get_entry(prev_idx)
                prev_term = prev_entry["term"] if prev_entry is not None else 0

            entries = self.log[next_idx - 1:]

            payload = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "leader_addr": self.addr,
                "prev_log_index": prev_idx,
                "prev_log_term": prev_term,
                "entries": entries,
                "leader_commit": self.commit_index,
            }

        try:
            resp = post_json(peer, "/append_entries", payload, timeout=1.0)
        except Exception:
            return False

        with self.lock:
            their_term = int(resp.get("term", 0))
            if their_term > self.current_term:
                self._become_follower(their_term, "")
                return False

            if self.role != LEADER:
                return False

            if bool(resp.get("success")):
                match_idx = int(resp.get("match_index", 0))
                self.match_index[peer] = match_idx
                self.next_index[peer] = match_idx + 1
                self._maybe_advance_commit()
                return True

            # Follower is behind or has conflicting suffix, so back up.
            self.next_index[peer] = max(1, self.next_index.get(peer, 1) - 1)
            return False
# BEGIN
    def _replicate_until_committed(self, target_index, timeout=5.0):
        # Used when leader appends a new client message.
        # Keep trying replication until committed or timeout.
        deadline = time.time() + timeout

        while time.time() < deadline and not self.stop_event.is_set():
            with self.lock:
                if self.role != LEADER:
                    return False

                if self.commit_index >= target_index:
                    self._apply_committed_entries()
                    return True

            threads = []
            for peer in list(self.peer_addrs):
                t = threading.Thread(
                    target=self._send_append_entries_to_peer,
                    args=(peer,),
                    daemon=True,
                )
                threads.append(t)
                t.start()

            for t in threads:
                t.join(timeout=1.2)

            with self.lock:
                self._maybe_advance_commit()
                self._apply_committed_entries()
                if self.commit_index >= target_index:
                    return True

            time.sleep(0.05)

        return False
# END

    # -------------------------------------------------
    # Internal helpers for the gateway
    # -------------------------------------------------
    def handle_client_append(self, data):
        # Gateway should call this on the leader when sending a message.
        with self.lock:
            if self.role != LEADER:
                return {
                    "ok": False,
                    "error": "not_leader",
                    "leader_hint": self.leader_hint,
                }

            dedup_key = (str(data["client_id"]), str(data["client_msg_id"]))

            # If this client request was already committed, return same seq.
            if dedup_key in self.seen_messages:
                return {
                    "ok": True,
                    "seq": self.seen_messages[dedup_key],
                    "leader_hint": self.addr,
                    "duplicate": True,
                }

            user_a, user_b = ordered_users(str(data["from_user"]), str(data["to_user"]))

            entry = {
                "index": self._last_log_index() + 1,
                "term": self.current_term,
                "from_user": str(data["from_user"]),
                "to_user": str(data["to_user"]),
                "user_a": user_a,
                "user_b": user_b,
                "text": str(data["text"]),
                "client_id": str(data["client_id"]),
                "client_msg_id": str(data["client_msg_id"]),
                "server_time_ms": now_ms(),
            }

            # Leader appends locally first, then replicates to followers.
            self.log.append(entry)

        ok = self._replicate_until_committed(entry["index"], timeout=5.0)

        with self.lock:
            if not ok:
                return {
                    "ok": False,
                    "error": "quorum_unavailable",
                    "leader_hint": self.addr if self.role == LEADER else self.leader_hint,
                }

            return {
                "ok": True,
                "seq": entry["index"],
                "leader_hint": self.addr,
                "duplicate": False,
            }

    def handle_read_history(self, data):
        # Gateway can call this on a replica to read committed conversation history.
        with self.lock:
            user_a = str(data["user_a"])
            user_b = str(data["user_b"])
            after_seq = int(data.get("after_seq", 0))
            limit = int(data.get("limit", 100))

            conv = list(self.messages.get((user_a, user_b), []))
            conv = [e for e in conv if int(e["seq"]) > after_seq]
            conv = conv[-limit:]

            return {
                "ok": True,
                "events": conv,
                "served_by": [self.addr],
                "commit_index": self.commit_index,
                "role": self.role,
                "leader_hint": self.leader_hint,
            }

class InternalHandler(BaseHTTPRequestHandler):
    # Tiny internal HTTP server for: RequestVote, AppendEntries, leader append from gateway, replica reads from gateway

    node = None

    def log_message(self, fmt, *args):
        # sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), fmt%args))
        return

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.node is None:
            self._send_json({"error": "node not ready"}, status=500)
            return

        try:
            data = self._read_json()
            # print(f"DEBUG: Received {self.path} - Data: {data}")

            if self.path == "/request_vote":
                self._send_json(self.node.handle_request_vote(data))
                return

            if self.path == "/append_entries":
                self._send_json(self.node.handle_append_entries(data))
                # resp = self.node.handle_append_entries(data)
                # print(f"DEBUG: append_entries response: {resp}")
                # self._send_json(resp)
                return

            if self.path == "/client_append":
                self._send_json(self.node.handle_client_append(data))
                # resp = self.node.handle_client_append(data)
                # print(f"DEBUG: client_append response: {resp}")
                # self._send_json(resp)
                return

            if self.path == "/read_history":
                self._send_json(self.node.handle_read_history(data))
                # resp = self.node.handle_read_history(data)
                # print(f"DEBUG: read_history response: {resp}")
                # self._send_json(resp)
                return

            if self.path == "/ping":
                self._send_json({"ok": True, "addr": self.node.addr})
                return

            self._send_json({"error": "unknown path"}, status=404)
        except Exception as e:
            self._send_json({"error": f"{type(e).__name__}: {e}"}, status=500)




def serve(host, port):
    node = ReplicaAdmin(host, port)

    # Internal server for Raft traffic and gateway access.
    InternalHandler.node = node
    http_server = ThreadingHTTPServer((host, port + 1000), InternalHandler)
    http_thread = threading.Thread(target=http_server.serve_forever, daemon=True)
    http_thread.start()

    # Required gRPC server that exposes ReplicaAdmin.Status.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replica_admin_pb2_grpc.add_ReplicaAdminServicer_to_server(node, server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()

    print(f"Replica {node.node_id} started on {host}:{port}")

    try:
        server.wait_for_termination()
    finally:
        node.stop_event.set()
        try:
            http_server.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    serve(args.host, args.port)
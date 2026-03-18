#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
import signal
import sys
import threading
import time
from concurrent import futures

import grpc

# `PROJECT_ROOT` is this file's directory and is used to locate generated
# protobuf modules before the imports execute.
PROJECT_ROOT = __file__.rsplit("/", 1)[0]
# `GENERATED_DIRECTORY` points at the generated gRPC Python package imported by
# this module during replica startup.
GENERATED_DIRECTORY = f"{PROJECT_ROOT}/generated"
if GENERATED_DIRECTORY not in sys.path:
    sys.path.insert(0, GENERATED_DIRECTORY)

import replica_admin_pb2
import replica_admin_pb2_grpc

from raft_support import (
    AppendEntriesMessage,
    AppendEntriesReply,
    ClientWriteMessage,
    ClientWriteReply,
    DEFAULT_REPLICA_COUNT,
    DEFAULT_REPLICA_START_PORT,
    LogEntry,
    ReadConversationMessage,
    ReadConversationReply,
    ReplicaAvailabilityMessage,
    ReplicaAvailabilityReply,
    ReplicaTransportPool,
    RequestVoteMessage,
    RequestVoteReply,
    StoredDirectEvent,
    create_dataclass_rpc_handler,
    current_time_millis,
    infer_replica_addresses,
    infer_replica_id_from_port,
    ordered_conversation_participants,
)


# Human-readable role label used when the replica is passively following a leader.
FOLLOWER_ROLE_NAME = "FOLLOWER"
# Human-readable role label used while the replica is campaigning for leadership.
CANDIDATE_ROLE_NAME = "CANDIDATE"
# Human-readable role label used while the replica is the active leader.
LEADER_ROLE_NAME = "LEADER"


class ReplicaStateMachine:
    """
    A compact Raft implementation tuned for the course test contract.

    The comments are intentionally dense and explicit because the assignment
    asks for code that students can read line-by-line while learning how
    elections, log matching, commit, and recovery interact.
    """

    def __init__(self, replica_id: int, listen_address: str, all_replica_addresses: list[str], start_in_disabled_mode: bool = False):
        """
        Initialize all in-memory Raft state and local bookkeeping for one replica.

        Inputs:
        - `replica_id`: numeric id derived from the configured port.
        - `listen_address`: host:port peers and scripts use to reach this replica.
        - `all_replica_addresses`: every configured replica address in the
          cluster, including this replica.
        - `start_in_disabled_mode`: whether the process should begin as an
          administratively stopped placeholder.

        Output:
        - No return value. The constructor prepares state used later by RPC
          handlers and background threads.

        Called by:
        - `main()` when the replica process starts.
        """
        self.replica_id = replica_id
        self.listen_address = listen_address
        self.all_replica_addresses = list(all_replica_addresses)
        self.peer_addresses = [address for address in self.all_replica_addresses if address != self.listen_address]

        self.state_lock = threading.RLock()
        self.state_changed = threading.Condition(self.state_lock)
        self.replication_lock = threading.Lock()
        self.stop_requested = threading.Event()
        self.transport_pool = ReplicaTransportPool()

        # Raft persistent state in the paper is "persistent".
        # The assignment explicitly says storage is in-memory only, so these values
        # are lost on crash/restart and reconstructed through Raft traffic.
        self.current_term = 0
        self.voted_for_replica_id: int | None = None
        self.log_entries: list[LogEntry] = [LogEntry(index=0, term=0, command_name="SENTINEL", payload={})]

        # Volatile leader / follower bookkeeping.
        self.role_name = FOLLOWER_ROLE_NAME
        self.known_leader_address = ""
        self.commit_index = 0
        self.last_applied_index = 0

        # Leader-only replication progress maps.
        self.next_index_by_peer_address: dict[str, int] = {}
        self.match_index_by_peer_address: dict[str, int] = {}

        # State machine materialized view used to answer reads quickly.
        self.events_by_conversation: dict[tuple[str, str], list[StoredDirectEvent]] = {}
        self.committed_sequence_by_client_message_key: dict[tuple[str, str], int] = {}
        self.pending_sequence_by_client_message_key: dict[tuple[str, str], int] = {}

        self._reset_election_deadline_locked()

        # A disabled replica is the process shape used by stop_replica.py.
        # It keeps the admin port alive so the tests can still call Status on every
        # configured address, but it refuses to vote, replicate, or serve reads.
        self.disabled_mode_enabled = start_in_disabled_mode
        self.rejoin_ready_time_monotonic = 0.0 if start_in_disabled_mode else (time.monotonic() + 0.75)
        self._log(
            f"started on {self.listen_address} with peers={self.peer_addresses} disabled={self.disabled_mode_enabled}"
        )

    def start_background_threads(self) -> None:
        """
        Start the election and heartbeat loops for an active replica.

        Inputs:
        - No explicit parameters beyond `self`.

        Output:
        - No return value. The method spawns daemon threads unless the replica
          is intentionally starting in disabled mode.

        Called by:
        - `main()` after it constructs `ReplicaStateMachine`.
        """
        if self.disabled_mode_enabled:
            return
        threading.Thread(target=self._run_election_timer_loop, name=f"replica-{self.replica_id}-election", daemon=True).start()
        threading.Thread(target=self._run_heartbeat_loop, name=f"replica-{self.replica_id}-heartbeat", daemon=True).start()

    def stop_background_threads(self) -> None:
        """
        Signal background loops to stop and close all peer RPC transports.

        Inputs:
        - No explicit parameters beyond `self`.

        Output:
        - No return value. The method wakes waiting threads and releases gRPC
          client resources during process shutdown.

        Called by:
        - `main()` in the server shutdown `finally` block.
        """
        self.stop_requested.set()
        with self.state_lock:
            self.state_changed.notify_all()
        self.transport_pool.close_all()

    def build_status_response(self) -> replica_admin_pb2.StatusResponse:
        """
        Snapshot the replica's current externally visible status.

        Inputs:
        - No explicit parameters beyond `self`.

        Output:
        - Returns a `replica_admin_pb2.StatusResponse` describing id, role, term,
          leader hint, and log/commit progress.

        Called by:
        - `ReplicaAdminService.Status()` when tests or scripts issue the public
          status RPC.
        """
        with self.state_lock:
            return replica_admin_pb2.StatusResponse(
                id=self.replica_id,
                role=self._role_name_to_proto_enum(self.role_name),
                term=self.current_term,
                leader_hint=self.known_leader_address,
                last_log_index=self._last_log_index_locked(),
                last_log_term=self._last_log_term_locked(),
                commit_index=self.commit_index,
            )

    def handle_request_vote(self, request_message: RequestVoteMessage) -> RequestVoteReply:
        """
        Process one Raft `RequestVote` RPC from a candidate replica.

        Inputs:
        - `request_message`: candidate term/id plus its last log index and term.

        Output:
        - Returns `RequestVoteReply` indicating the responder's term and whether
          the vote was granted.

        Called by:
        - The generic internal RPC handler registered in `main()` for
          `raft.Internal/RequestVote`.
        - Peer replicas reach that callsite from `_run_election_timer_loop()`.
        """
        with self.state_lock:
            self._log(
                f"received RequestVote from replica {request_message.candidate_id} "
                f"term={request_message.term} last_log=({request_message.last_log_index}, {request_message.last_log_term})"
            )
            if self._raft_participation_is_paused_locked():
                self._log("rejecting RequestVote because this replica is paused")
                return RequestVoteReply(term=self.current_term, vote_granted=False)

            if request_message.term < self.current_term:
                self._log(
                    f"rejecting RequestVote from replica {request_message.candidate_id} "
                    f"because candidate term {request_message.term} is older than current term {self.current_term}"
                )
                return RequestVoteReply(term=self.current_term, vote_granted=False)

            if request_message.term > self.current_term:
                self._become_follower_locked(new_term=request_message.term, leader_address="")

            candidate_log_is_up_to_date = self._candidate_log_is_up_to_date_locked(
                candidate_last_log_index=request_message.last_log_index,
                candidate_last_log_term=request_message.last_log_term,
            )
            can_vote_for_candidate = self.voted_for_replica_id in (None, request_message.candidate_id)

            if can_vote_for_candidate and candidate_log_is_up_to_date:
                self.voted_for_replica_id = request_message.candidate_id
                self._reset_election_deadline_locked()
                self._log(
                    f"granted vote to replica {request_message.candidate_id} for term {request_message.term}"
                )
                return RequestVoteReply(term=self.current_term, vote_granted=True)

            self._log(
                f"rejecting vote for replica {request_message.candidate_id}; "
                f"can_vote={can_vote_for_candidate} candidate_log_up_to_date={candidate_log_is_up_to_date}"
            )
            return RequestVoteReply(term=self.current_term, vote_granted=False)

    def handle_append_entries(self, request_message: AppendEntriesMessage) -> AppendEntriesReply:
        """
        Process one Raft `AppendEntries` RPC carrying heartbeats or log entries.

        Inputs:
        - `request_message`: leader metadata, previous-log match point, new
          entries, and the leader's commit index.

        Output:
        - Returns `AppendEntriesReply` indicating the current term, whether the
          append succeeded, and the best known matching log index.

        Called by:
        - The generic internal RPC handler registered in `main()` for
          `raft.Internal/AppendEntries`.
        - Peer leaders reach that callsite from `_run_one_replication_round()`.
        """
        with self.state_lock:
            if request_message.entries:
                self._log(
                    f"received AppendEntries from leader {request_message.leader_id} at {request_message.leader_address} "
                    f"term={request_message.term} prev_index={request_message.prev_log_index} "
                    f"entry_count={len(request_message.entries)} leader_commit={request_message.leader_commit}"
                )
            if self._raft_participation_is_paused_locked():
                if request_message.entries:
                    self._log("rejecting AppendEntries because this replica is paused")
                return AppendEntriesReply(term=self.current_term, success=False, match_index=self._last_log_index_locked())

            if request_message.term < self.current_term:
                if request_message.entries:
                    self._log(
                        f"rejecting AppendEntries from {request_message.leader_address} because "
                        f"term {request_message.term} is older than current term {self.current_term}"
                    )
                return AppendEntriesReply(
                    term=self.current_term,
                    success=False,
                    match_index=self._last_log_index_locked(),
                )

            if request_message.term > self.current_term or self.role_name != FOLLOWER_ROLE_NAME:
                self._become_follower_locked(
                    new_term=request_message.term,
                    leader_address=request_message.leader_address,
                )
            else:
                self.known_leader_address = request_message.leader_address

            # Any valid AppendEntries, including heartbeat-only messages, proves that
            # there is an active leader for this term. Followers therefore reset their
            # election timer every time they accept one.
            self._reset_election_deadline_locked()

            if request_message.prev_log_index >= len(self.log_entries):
                if request_message.entries:
                    self._log(
                        f"rejecting AppendEntries because prev_log_index {request_message.prev_log_index} "
                        f"is beyond local log length {len(self.log_entries) - 1}"
                    )
                return AppendEntriesReply(
                    term=self.current_term,
                    success=False,
                    match_index=self._last_log_index_locked(),
                )

            if self.log_entries[request_message.prev_log_index].term != request_message.prev_log_term:
                conflicting_index = request_message.prev_log_index
                while conflicting_index > 0 and self.log_entries[conflicting_index].term == self.log_entries[request_message.prev_log_index].term:
                    conflicting_index -= 1
                if request_message.entries:
                    self._log(
                        f"rejecting AppendEntries because local term at index {request_message.prev_log_index} "
                        f"does not match prev_log_term {request_message.prev_log_term}; suggesting retry from {conflicting_index}"
                    )
                return AppendEntriesReply(
                    term=self.current_term,
                    success=False,
                    match_index=conflicting_index,
                )

            insertion_index = request_message.prev_log_index + 1
            incoming_entries = [LogEntry(**entry_dictionary) for entry_dictionary in request_message.entries]

            # Raft's log matching rule says that once two entries share the same
            # index and term, the prefix before them must match. We therefore only
            # need to compare the suffix starting at insertion_index and discard the
            # follower's conflicting tail if terms diverge.
            compare_index = 0
            while (
                compare_index < len(incoming_entries)
                and insertion_index + compare_index < len(self.log_entries)
                and self.log_entries[insertion_index + compare_index].term == incoming_entries[compare_index].term
            ):
                compare_index += 1

            if insertion_index + compare_index < len(self.log_entries):
                truncated_suffix_entries = self.log_entries[insertion_index + compare_index :]
                self.log_entries = self.log_entries[: insertion_index + compare_index]
                if truncated_suffix_entries:
                    self._log(
                        f"truncated {len(truncated_suffix_entries)} conflicting log entr{'y' if len(truncated_suffix_entries) == 1 else 'ies'} "
                        f"starting at index {truncated_suffix_entries[0].index}"
                    )

                # If we truncate away an uncommitted suffix, any pending dedup record
                # that pointed into that suffix must also be removed.
                truncated_indexes = {entry.index for entry in truncated_suffix_entries}
                for client_message_key, pending_sequence in list(self.pending_sequence_by_client_message_key.items()):
                    if pending_sequence in truncated_indexes:
                        self.pending_sequence_by_client_message_key.pop(client_message_key, None)

            for remaining_entry in incoming_entries[compare_index:]:
                self.log_entries.append(remaining_entry)
                payload = remaining_entry.payload
                client_message_key = (payload["client_id"], payload["client_msg_id"])
                self.pending_sequence_by_client_message_key[client_message_key] = remaining_entry.index
                self._log(
                    f"appended replicated message {payload['client_id']}:{payload['client_msg_id']} "
                    f"at log index {remaining_entry.index}"
                )

            if request_message.leader_commit > self.commit_index:
                self.commit_index = min(request_message.leader_commit, self._last_log_index_locked())
                self._apply_committed_entries_locked()

            self.state_changed.notify_all()
            return AppendEntriesReply(
                term=self.current_term,
                success=True,
                match_index=self._last_log_index_locked(),
            )

    def handle_client_write(self, request_message: ClientWriteMessage) -> ClientWriteReply:
        """
        Accept or reject one client write forwarded by the gateway.

        Inputs:
        - `request_message`: sender, recipient, client identity, client message
          id, and message text.

        Output:
        - Returns `ClientWriteReply` reporting whether the write committed, what
          term/leader was involved, and the committed sequence number if any.

        Called by:
        - The generic internal RPC handler registered in `main()` for
          `raft.Internal/ClientWrite`.
        - The gateway reaches that callsite from `DirectGatewayService.SendDirect`.
        """
        client_message_key = (request_message.client_id, request_message.client_msg_id)

        with self.state_lock:
            self._log(
                f"received ClientWrite from gateway for {request_message.from_user}->{request_message.to_user} "
                f"message={request_message.client_id}:{request_message.client_msg_id}"
            )
            if self._raft_participation_is_paused_locked():
                self._log("rejecting ClientWrite because this replica is paused")
                return ClientWriteReply(
                    accepted=False,
                    term=self.current_term,
                    leader_hint=self.known_leader_address,
                    seq=0,
                    error_message="replica is administratively stopped",
                )

            if self.role_name != LEADER_ROLE_NAME:
                self._log(
                    f"rejecting ClientWrite because this replica is not leader; leader_hint={self.known_leader_address or 'unknown'}"
                )
                return ClientWriteReply(
                    accepted=False,
                    term=self.current_term,
                    leader_hint=self.known_leader_address,
                    seq=0,
                    error_message="write must go to the current leader",
                )

            existing_committed_sequence = self.committed_sequence_by_client_message_key.get(client_message_key)
            if existing_committed_sequence is not None:
                self._log(
                    f"returning existing committed sequence {existing_committed_sequence} "
                    f"for duplicate client message {client_message_key}"
                )
                return ClientWriteReply(
                    accepted=True,
                    term=self.current_term,
                    leader_hint=self.listen_address,
                    seq=existing_committed_sequence,
                    error_message="",
                )

            pending_sequence = self.pending_sequence_by_client_message_key.get(client_message_key)
            if pending_sequence is None:
                next_log_index = self._last_log_index_locked() + 1
                next_log_entry = LogEntry(
                    index=next_log_index,
                    term=self.current_term,
                    command_name="SEND_DIRECT",
                    payload={
                        "from_user": request_message.from_user,
                        "to_user": request_message.to_user,
                        "client_id": request_message.client_id,
                        "client_msg_id": request_message.client_msg_id,
                        "text": request_message.text,
                    },
                )
                self.log_entries.append(next_log_entry)
                self.pending_sequence_by_client_message_key[client_message_key] = next_log_index
                pending_sequence = next_log_index
                self._log(
                    f"accepted new client write and appended it locally at log index {next_log_index}"
                )
            else:
                self._log(
                    f"client write is already pending at log index {pending_sequence}; waiting for commit"
                )

        write_committed = self._replicate_until_committed(target_log_index=pending_sequence, timeout_seconds=3.5)

        with self.state_lock:
            committed_sequence = self.committed_sequence_by_client_message_key.get(client_message_key)
            if write_committed and committed_sequence is not None:
                self._log(
                    f"committed client message {(request_message.client_id, request_message.client_msg_id)} at seq {committed_sequence}"
                )
                return ClientWriteReply(
                    accepted=True,
                    term=self.current_term,
                    leader_hint=self.listen_address,
                    seq=committed_sequence,
                    error_message="",
                )

            # If we can no longer reach a majority, the gateway should surface that to
            # the client instead of pretending the write succeeded.
            return ClientWriteReply(
                accepted=False,
                term=self.current_term,
                leader_hint=self.known_leader_address,
                seq=0,
                error_message="write could not be committed with the current quorum",
            )

    def handle_read_conversation(self, request_message: ReadConversationMessage) -> ReadConversationReply:
        """
        Serve a read-only conversation history request from the gateway.

        Inputs:
        - `request_message`: canonical user pair, lower sequence bound, and
          optional result limit.

        Output:
        - Returns `ReadConversationReply` containing accept/reject status,
          leader hint, serving replica address, and any matching events.

        Called by:
        - The generic internal RPC handler registered in `main()` for
          `raft.Internal/ReadConversation`.
        - The gateway reaches that callsite from
          `DirectGatewayService.GetConversationHistory`.
        """
        with self.state_lock:
            self._log(
                f"received ReadConversation from gateway for users=({request_message.user_a}, {request_message.user_b}) "
                f"after_seq={request_message.after_seq} limit={request_message.limit}"
            )
            if self._raft_participation_is_paused_locked():
                self._log("rejecting ReadConversation because this replica is paused")
                return ReadConversationReply(
                    accepted=False,
                    term=self.current_term,
                    leader_hint=self.known_leader_address,
                    served_by=[],
                    events=[],
                    error_message="replica is administratively stopped",
                )

            if self.last_applied_index < self.commit_index:
                self._log(
                    f"rejecting ReadConversation because apply lag exists "
                    f"(last_applied_index={self.last_applied_index}, commit_index={self.commit_index})"
                )
                return ReadConversationReply(
                    accepted=False,
                    term=self.current_term,
                    leader_hint=self.known_leader_address,
                    served_by=[],
                    events=[],
                    error_message="replica is still applying committed entries",
                )

            conversation_key = ordered_conversation_participants(request_message.user_a, request_message.user_b)
            stored_events = self.events_by_conversation.get(conversation_key, [])
            filtered_events = [
                event
                for event in stored_events
                if event.seq > request_message.after_seq
            ]
            limited_events = filtered_events[: request_message.limit or len(filtered_events)]
            self._log(f"serving read with {len(limited_events)} event(s) from {self.listen_address}")

            return ReadConversationReply(
                accepted=True,
                term=self.current_term,
                leader_hint=self.known_leader_address or self.listen_address,
                served_by=[self.listen_address],
                events=[
                    {
                        "seq": event.seq,
                        "from_user": event.from_user,
                        "text": event.text,
                        "server_time_ms": event.server_time_ms,
                        "client_id": event.client_id,
                        "client_msg_id": event.client_msg_id,
                    }
                    for event in limited_events
                ],
                error_message="",
            )

    def handle_set_replica_availability(self, request_message: ReplicaAvailabilityMessage) -> ReplicaAvailabilityReply:
        """
        Toggle this replica between active mode and administrative stop mode.

        Inputs:
        - `request_message`: boolean pause flag where `True` means stop and
          `False` means resume.

        Output:
        - Returns `ReplicaAvailabilityReply(acknowledged=True)` after applying
          the requested state change.

        Called by:
        - The generic internal RPC handler registered in `main()` for
          `raft.Internal/SetReplicaAvailability`.
        - Cluster management scripts such as `stop_replica.py` and
          `start_replica.py`.
        """
        self._log(f"received SetReplicaAvailability paused={request_message.paused}")
        if request_message.paused:
            self.enter_disabled_mode()
        else:
            self.exit_disabled_mode()
        return ReplicaAvailabilityReply(acknowledged=True)

    def _run_election_timer_loop(self) -> None:
        """
        Background thread that starts elections when the leader appears absent.

        Inputs:
        - No explicit parameters beyond `self`.

        Output:
        - No return value during normal operation. The loop exits when
          `self.stop_requested` is set.

        Called by:
        - `start_background_threads()` in a daemon thread.
        """
        while not self.stop_requested.is_set():
            time.sleep(0.05)

            with self.state_lock:
                if self._raft_participation_is_paused_locked():
                    continue
                if self.role_name == LEADER_ROLE_NAME:
                    continue

                if time.monotonic() < self.election_deadline_monotonic:
                    continue

                self.current_term += 1
                self.role_name = CANDIDATE_ROLE_NAME
                self.voted_for_replica_id = self.replica_id
                self.known_leader_address = ""
                self._reset_election_deadline_locked()
                self._log(f"starting election for term {self.current_term}")

                candidate_term = self.current_term
                last_log_index = self._last_log_index_locked()
                last_log_term = self._last_log_term_locked()

            granted_votes = 1
            majority_vote_count = self._majority_count()

            # Leave a short observation window where the node is visibly a candidate.
            # The tests intentionally look for this transient state.
            time.sleep(0.2)

            for peer_address in self.peer_addresses:
                if self.stop_requested.is_set():
                    return

                self._log(
                    f"requesting vote from {peer_address} for term {candidate_term} "
                    f"with last_log=({last_log_index}, {last_log_term})"
                )
                try:
                    peer_reply = self.transport_pool.get_client(peer_address).request_vote(
                        RequestVoteMessage(
                            term=candidate_term,
                            candidate_id=self.replica_id,
                            last_log_index=last_log_index,
                            last_log_term=last_log_term,
                        ),
                        timeout_seconds=0.08,
                    )
                except grpc.RpcError:
                    self._log(f"vote request to {peer_address} failed at transport level")
                    continue

                with self.state_lock:
                    if peer_reply.term > self.current_term:
                        self._become_follower_locked(new_term=peer_reply.term, leader_address="")
                        break

                    if self.role_name != CANDIDATE_ROLE_NAME or self.current_term != candidate_term:
                        break

                    if peer_reply.vote_granted:
                        granted_votes += 1
                    self._log(
                        f"vote reply from {peer_address}: granted={peer_reply.vote_granted} "
                        f"term={peer_reply.term} total_votes={granted_votes}"
                    )

                    if granted_votes >= majority_vote_count:
                        self._become_leader_locked()
                        break

    def _run_heartbeat_loop(self) -> None:
        """
        Background thread that keeps followers updated while this replica is leader.

        Inputs:
        - No explicit parameters beyond `self`.

        Output:
        - No return value during normal operation. The loop exits when
          `self.stop_requested` is set.

        Called by:
        - `start_background_threads()` in a daemon thread.
        """
        while not self.stop_requested.is_set():
            should_send_heartbeats = False
            with self.state_lock:
                should_send_heartbeats = self.role_name == LEADER_ROLE_NAME and not self._raft_participation_is_paused_locked()

            if should_send_heartbeats:
                self._run_one_replication_round()
                time.sleep(0.05)
            else:
                time.sleep(0.05)

    def _replicate_until_committed(self, target_log_index: int, timeout_seconds: float) -> bool:
        """
        Keep running replication rounds until a target log index is committed.

        Inputs:
        - `target_log_index`: the leader log index that should reach commit.
        - `timeout_seconds`: maximum amount of time to wait.

        Output:
        - Returns `True` if the target index becomes committed before timeout and
          `False` otherwise.

        Called by:
        - `handle_client_write()` after the leader appends a new client message.
        """
        deadline_monotonic = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline_monotonic and not self.stop_requested.is_set():
            with self.state_lock:
                if self.role_name != LEADER_ROLE_NAME:
                    return False
                if self.commit_index >= target_log_index:
                    return True

            self._run_one_replication_round()

            with self.state_lock:
                if self.commit_index >= target_log_index:
                    return True
                self.state_changed.wait(timeout=0.05)

        with self.state_lock:
            return self.commit_index >= target_log_index

    def _run_one_replication_round(self) -> None:
        """
        Send one AppendEntries pass to every peer and update match/next indexes.

        Inputs:
        - No explicit parameters beyond `self`.

        Output:
        - No direct return value. The method mutates replication state and may
          advance commit progress.

        Called by:
        - `_run_heartbeat_loop()` for periodic leader heartbeats.
        - `_replicate_until_committed()` while waiting on a client write.
        """
        if not self.replication_lock.acquire(blocking=False):
            return

        try:
            for peer_address in self.peer_addresses:
                with self.state_lock:
                    if self.role_name != LEADER_ROLE_NAME:
                        return

                    next_index_for_peer = self.next_index_by_peer_address.get(peer_address, self._last_log_index_locked() + 1)
                    previous_log_index = max(0, next_index_for_peer - 1)
                    previous_log_term = self.log_entries[previous_log_index].term
                    entries_to_send = [
                        {
                            "index": entry.index,
                            "term": entry.term,
                            "command_name": entry.command_name,
                            "payload": dict(entry.payload),
                        }
                        for entry in self.log_entries[next_index_for_peer:]
                    ]
                    current_term = self.current_term
                    current_commit_index = self.commit_index

                try:
                    if entries_to_send:
                        self._log(
                            f"sending {len(entries_to_send)} replicated entr{'y' if len(entries_to_send) == 1 else 'ies'} "
                            f"to {peer_address} starting at index {entries_to_send[0]['index']}"
                        )
                    peer_reply = self.transport_pool.get_client(peer_address).append_entries(
                        AppendEntriesMessage(
                            term=current_term,
                            leader_id=self.replica_id,
                            leader_address=self.listen_address,
                            prev_log_index=previous_log_index,
                            prev_log_term=previous_log_term,
                            entries=entries_to_send,
                            leader_commit=current_commit_index,
                        ),
                        timeout_seconds=0.08,
                    )
                except grpc.RpcError:
                    if entries_to_send:
                        self._log(f"replication RPC to {peer_address} failed at transport level")
                    continue

                with self.state_lock:
                    if peer_reply.term > self.current_term:
                        self._become_follower_locked(new_term=peer_reply.term, leader_address="")
                        return

                    if self.role_name != LEADER_ROLE_NAME or current_term != self.current_term:
                        return

                    if peer_reply.success:
                        self.match_index_by_peer_address[peer_address] = peer_reply.match_index
                        self.next_index_by_peer_address[peer_address] = peer_reply.match_index + 1
                        if entries_to_send:
                            self._log(
                                f"{peer_address} accepted replication through index {peer_reply.match_index}"
                            )
                    else:
                        current_next_index = self.next_index_by_peer_address.get(peer_address, self._last_log_index_locked() + 1)
                        self.next_index_by_peer_address[peer_address] = max(1, min(current_next_index - 1, peer_reply.match_index + 1))
                        if entries_to_send:
                            self._log(
                                f"{peer_address} rejected replication; backing next index down to "
                                f"{self.next_index_by_peer_address[peer_address]}"
                            )

            with self.state_lock:
                self._advance_commit_index_locked()
        finally:
            self.replication_lock.release()

    def _advance_commit_index_locked(self) -> None:
        """
        Advance the leader commit index once a current-term entry has majority support.

        Inputs:
        - No explicit parameters beyond `self`. Caller must already hold
          `self.state_lock`.

        Output:
        - No return value. The method may update `self.commit_index`, apply new
          entries, and wake waiting threads.

        Called by:
        - `_run_one_replication_round()` after collecting peer replies.
        """
        if self.role_name != LEADER_ROLE_NAME:
            return

        last_log_index = self._last_log_index_locked()
        majority_vote_count = self._majority_count()

        # The leader can only commit entries from its current term after a majority
        # confirms them. This mirrors Raft's safety rule and prevents an older term's
        # uncommitted suffix from being incorrectly declared committed.
        for candidate_commit_index in range(last_log_index, self.commit_index, -1):
            if self.log_entries[candidate_commit_index].term != self.current_term:
                continue

            replicated_count = 1
            for peer_address in self.peer_addresses:
                if self.match_index_by_peer_address.get(peer_address, 0) >= candidate_commit_index:
                    replicated_count += 1

            if replicated_count >= majority_vote_count:
                self.commit_index = candidate_commit_index
                self._apply_committed_entries_locked()
                self.state_changed.notify_all()
                return

    def _apply_committed_entries_locked(self) -> None:
        """
        Apply newly committed log entries into the materialized chat state.

        Inputs:
        - No explicit parameters beyond `self`. Caller must already hold
          `self.state_lock`.

        Output:
        - No return value. The method updates read-serving structures and clears
          pending dedup records for applied client messages.

        Called by:
        - `handle_append_entries()` when a follower learns a higher commit index.
        - `_advance_commit_index_locked()` when the leader commits an entry.
        """
        while self.last_applied_index < self.commit_index:
            self.last_applied_index += 1
            log_entry = self.log_entries[self.last_applied_index]

            if log_entry.command_name != "SEND_DIRECT":
                continue

            payload = log_entry.payload
            conversation_key = ordered_conversation_participants(payload["from_user"], payload["to_user"])
            client_message_key = (payload["client_id"], payload["client_msg_id"])

            if client_message_key not in self.committed_sequence_by_client_message_key:
                stored_event = StoredDirectEvent(
                    seq=log_entry.index,
                    from_user=payload["from_user"],
                    text=payload["text"],
                    server_time_ms=current_time_millis(),
                    client_id=payload["client_id"],
                    client_msg_id=payload["client_msg_id"],
                )
                self.events_by_conversation.setdefault(conversation_key, []).append(stored_event)
                self.committed_sequence_by_client_message_key[client_message_key] = log_entry.index

            self.pending_sequence_by_client_message_key.pop(client_message_key, None)

    def _candidate_log_is_up_to_date_locked(self, candidate_last_log_index: int, candidate_last_log_term: int) -> bool:
        """
        Compare a candidate's advertised log tip against the local log tip.

        Inputs:
        - `candidate_last_log_index`: candidate's last log index.
        - `candidate_last_log_term`: candidate's last log term.

        Output:
        - Returns `True` when the candidate is at least as up to date as this
          replica according to the Raft voting rule.

        Called by:
        - `handle_request_vote()` before deciding whether to grant a vote.
        """
        local_last_log_term = self._last_log_term_locked()
        if candidate_last_log_term != local_last_log_term:
            return candidate_last_log_term > local_last_log_term
        return candidate_last_log_index >= self._last_log_index_locked()

    def _become_follower_locked(self, new_term: int, leader_address: str) -> None:
        """
        Transition this replica into follower mode for the given term.

        Inputs:
        - `new_term`: the newer term that should become current.
        - `leader_address`: best known leader address for that term, or empty
          string if unknown.

        Output:
        - No return value. The method resets vote state, updates role metadata,
          and refreshes the election deadline.

        Called by:
        - `handle_request_vote()`, `handle_append_entries()`,
          `_run_election_timer_loop()`, and `_run_one_replication_round()` when a
          newer term is observed or leadership is relinquished.
        """
        previous_role_name = self.role_name
        previous_term = self.current_term
        self.current_term = new_term
        self.role_name = FOLLOWER_ROLE_NAME
        self.voted_for_replica_id = None
        self.known_leader_address = leader_address
        self._reset_election_deadline_locked()
        self.state_changed.notify_all()
        self._log(
            f"became follower (from role={previous_role_name}, term={previous_term}) "
            f"-> term={self.current_term}, leader_hint={leader_address or 'unknown'}"
        )

    def _become_leader_locked(self) -> None:
        """
        Transition this candidate into leader mode and initialize peer tracking.

        Inputs:
        - No explicit parameters beyond `self`. Caller must already hold
          `self.state_lock`.

        Output:
        - No return value. The method updates role metadata and resets the
          replication progress maps for each follower.

        Called by:
        - `_run_election_timer_loop()` after a candidate gathers a majority.
        """
        self.role_name = LEADER_ROLE_NAME
        self.known_leader_address = self.listen_address
        next_index = self._last_log_index_locked() + 1
        self.next_index_by_peer_address = {peer_address: next_index for peer_address in self.peer_addresses}
        self.match_index_by_peer_address = {peer_address: 0 for peer_address in self.peer_addresses}
        self.state_changed.notify_all()
        self._log(f"became leader for term {self.current_term}")

    def _last_log_index_locked(self) -> int:
        """
        Return the index of the last local log entry.

        Inputs:
        - No explicit parameters beyond `self`. Caller is expected to already
          hold `self.state_lock`.

        Output:
        - Returns the integer index from the tail `LogEntry`.

        Called by:
        - Many helper methods and RPC handlers that need the current log tip.
        """
        return self.log_entries[-1].index

    def _last_log_term_locked(self) -> int:
        """
        Return the term of the last local log entry.

        Inputs:
        - No explicit parameters beyond `self`. Caller is expected to already
          hold `self.state_lock`.

        Output:
        - Returns the integer term from the tail `LogEntry`.

        Called by:
        - `build_status_response()`, `handle_request_vote()`,
          `_run_election_timer_loop()`, and `_candidate_log_is_up_to_date_locked()`.
        """
        return self.log_entries[-1].term

    def _majority_count(self) -> int:
        """
        Compute how many replicas are required for quorum in this cluster.

        Inputs:
        - No explicit parameters beyond `self`.

        Output:
        - Returns the integer majority threshold `(N // 2) + 1`.

        Called by:
        - `_run_election_timer_loop()` and `_advance_commit_index_locked()`.
        """
        return (len(self.all_replica_addresses) // 2) + 1

    def _reset_election_deadline_locked(self) -> None:
        """
        Pick a new randomized election timeout deadline for this replica.

        Inputs:
        - No explicit parameters beyond `self`. Caller is expected to already
          hold `self.state_lock`.

        Output:
        - No return value. The method updates `self.election_deadline_monotonic`.

        Called by:
        - `__init__()`, `handle_request_vote()`, `handle_append_entries()`,
          `_run_election_timer_loop()`, `_become_follower_locked()`, and
          `exit_disabled_mode()`.
        """
        self.election_deadline_monotonic = time.monotonic() + random.uniform(1.4, 2.3)

    def _raft_participation_is_paused_locked(self) -> bool:
        """
        Report whether the replica should temporarily refuse Raft participation.

        Inputs:
        - No explicit parameters beyond `self`. Caller is expected to already
          hold `self.state_lock`.

        Output:
        - Returns `True` when the replica is administratively disabled or still
          inside its post-restart catch-up delay.

        Called by:
        - RPC handlers and background loops before they vote, replicate, or
          serve reads.
        """
        return self.disabled_mode_enabled or time.monotonic() < self.rejoin_ready_time_monotonic

    def enter_disabled_mode(self) -> None:
        """
        Move the replica into administrative stop mode without killing the process.

        Inputs:
        - No explicit parameters beyond `self`.

        Output:
        - No return value. The replica remains reachable for admin/status RPCs
          but stops participating in Raft and read service.

        Called by:
        - `handle_set_replica_availability()`.
        - The `SIGUSR1` signal handler installed in `main()`.
        """
        with self.state_lock:
            self.disabled_mode_enabled = True
            self.role_name = FOLLOWER_ROLE_NAME
            self.known_leader_address = ""
            self.state_changed.notify_all()
            self._log("entered administrative stop mode")

    def exit_disabled_mode(self) -> None:
        """
        Leave administrative stop mode and schedule a short catch-up delay.

        Inputs:
        - No explicit parameters beyond `self`.

        Output:
        - No return value. The replica becomes eligible to rejoin after the
          delay stored in `self.rejoin_ready_time_monotonic`.

        Called by:
        - `handle_set_replica_availability()`.
        - The `SIGUSR2` signal handler installed in `main()`.
        """
        with self.state_lock:
            self.disabled_mode_enabled = False
            self.role_name = FOLLOWER_ROLE_NAME
            self.known_leader_address = ""
            self.rejoin_ready_time_monotonic = time.monotonic() + 1.0
            self._reset_election_deadline_locked()
            self.state_changed.notify_all()
            self._log("left administrative stop mode and will rejoin after catch-up delay")

    def _log(self, message: str) -> None:
        """
        Emit one human-readable diagnostic line for manual runs.

        Inputs:
        - `message`: already formatted text describing the event.

        Output:
        - No return value. The method writes a prefixed line to `stderr`.

        Called by:
        - Most RPC handlers, background loops, and `main()` lifecycle events.
        """
        print(f"[replica {self.replica_id} @ {self.listen_address}] {message}", file=sys.stderr, flush=True)

    @staticmethod
    def _role_name_to_proto_enum(role_name: str) -> int:
        """
        Translate a local role string into the protobuf enum used by Status RPCs.

        Inputs:
        - `role_name`: one of the local role constants defined at module scope.

        Output:
        - Returns the corresponding `replica_admin_pb2` enum integer.

        Called by:
        - `build_status_response()` when constructing the public admin reply.
        """
        if role_name == FOLLOWER_ROLE_NAME:
            return replica_admin_pb2.FOLLOWER
        if role_name == CANDIDATE_ROLE_NAME:
            return replica_admin_pb2.CANDIDATE
        return replica_admin_pb2.LEADER


class ReplicaAdminService(replica_admin_pb2_grpc.ReplicaAdminServicer):
    """
    Thin public gRPC service wrapper that exposes only the `Status` RPC.

    The internal Raft RPCs are registered separately in `main()` using generic
    handlers, because those RPCs exchange dataclass payloads rather than the
    generated public admin messages.
    """

    def __init__(self, replica_state_machine: ReplicaStateMachine):
        """
        Store the already-initialized replica state machine used by RPC methods.

        Inputs:
        - `replica_state_machine`: the local `ReplicaStateMachine` instance that
          owns all replica behavior and state.

        Output:
        - No return value.

        Called by:
        - `main()` when wiring the gRPC server.
        """
        self.replica_state_machine = replica_state_machine

    def Status(self, request, context):
        """
        Answer the public admin `Status` RPC with the latest replica snapshot.

        Inputs:
        - `request`: unused protobuf `StatusRequest`.
        - `context`: gRPC RPC context provided by the server runtime.

        Output:
        - Returns `replica_admin_pb2.StatusResponse`.

        Called by:
        - External callers such as tests, cluster scripts, and gateway leader
          discovery through the generated `ReplicaAdminStub`.
        """
        return self.replica_state_machine.build_status_response()


def build_argument_parser() -> argparse.ArgumentParser:
    """
    Define the CLI accepted by the standalone replica process.

    Inputs:
    - No explicit Python arguments.

    Output:
    - Returns an `ArgumentParser` configured with host, port, replica-count, and
      disabled-mode options.

    Called by:
    - `main()` before parsing process arguments.
    """
    argument_parser = argparse.ArgumentParser(description="Run one Raft replica that also exposes the ReplicaAdmin Status RPC.")
    argument_parser.add_argument("--host", default="127.0.0.1")
    argument_parser.add_argument("--port", type=int, required=True)
    argument_parser.add_argument("--replica-count", type=int, default=DEFAULT_REPLICA_COUNT)
    argument_parser.add_argument("--replica-start-port", type=int, default=DEFAULT_REPLICA_START_PORT)
    argument_parser.add_argument("--disabled", action="store_true", help="Run a placeholder replica that only answers Status.")
    return argument_parser


def main() -> int:
    """
    Start the replica process, register RPC handlers, and block until shutdown.

    Inputs:
    - No explicit Python arguments. The function reads CLI options via
      `build_argument_parser().parse_args()`.

    Output:
    - Returns integer exit code `0` on a normal shutdown path. Startup binding
      failures raise an exception instead.

    Called by:
    - The module-level `if __name__ == "__main__"` block.
    """
    parsed_arguments = build_argument_parser().parse_args()

    replica_id = infer_replica_id_from_port(parsed_arguments.port, parsed_arguments.replica_start_port)
    listen_address = f"{parsed_arguments.host}:{parsed_arguments.port}"
    bind_address = f"0.0.0.0:{parsed_arguments.port}"
    all_replica_addresses = infer_replica_addresses(
        host=parsed_arguments.host,
        replica_count=parsed_arguments.replica_count,
        replica_start_port=parsed_arguments.replica_start_port,
    )

    replica_state_machine = ReplicaStateMachine(
        replica_id=replica_id,
        listen_address=listen_address,
        all_replica_addresses=all_replica_addresses,
        start_in_disabled_mode=parsed_arguments.disabled,
    )
    replica_state_machine.start_background_threads()
    replica_state_machine._log(f"starting gRPC server on {bind_address}")

    signal.signal(signal.SIGUSR1, lambda signum, frame: replica_state_machine.enter_disabled_mode())
    signal.signal(signal.SIGUSR2, lambda signum, frame: replica_state_machine.exit_disabled_mode())

    grpc_server = None
    last_bind_error: Exception | None = None
    for _ in range(25):
        try:
            grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
            replica_admin_pb2_grpc.add_ReplicaAdminServicer_to_server(ReplicaAdminService(replica_state_machine), grpc_server)

            internal_rpc_handlers = {
                "RequestVote": create_dataclass_rpc_handler(RequestVoteMessage, RequestVoteReply, lambda request, context: replica_state_machine.handle_request_vote(request)),
                "AppendEntries": create_dataclass_rpc_handler(AppendEntriesMessage, AppendEntriesReply, lambda request, context: replica_state_machine.handle_append_entries(request)),
                "ClientWrite": create_dataclass_rpc_handler(ClientWriteMessage, ClientWriteReply, lambda request, context: replica_state_machine.handle_client_write(request)),
                "ReadConversation": create_dataclass_rpc_handler(ReadConversationMessage, ReadConversationReply, lambda request, context: replica_state_machine.handle_read_conversation(request)),
                "SetReplicaAvailability": create_dataclass_rpc_handler(ReplicaAvailabilityMessage, ReplicaAvailabilityReply, lambda request, context: replica_state_machine.handle_set_replica_availability(request)),
            }
            grpc_server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("raft.Internal", internal_rpc_handlers),))

            grpc_server.add_insecure_port(bind_address)
            grpc_server.start()
            last_bind_error = None
            replica_state_machine._log("gRPC server is now listening")
            break
        except RuntimeError as bind_error:
            last_bind_error = bind_error
            if grpc_server is not None:
                grpc_server.stop(grace=0)
            time.sleep(0.2)

    if grpc_server is None or last_bind_error is not None:
        raise last_bind_error if last_bind_error is not None else RuntimeError(f"could not bind replica server to {bind_address}")

    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        replica_state_machine._log("received KeyboardInterrupt, shutting down")
    finally:
        replica_state_machine.stop_background_threads()
        grpc_server.stop(grace=None)
        replica_state_machine._log("server stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

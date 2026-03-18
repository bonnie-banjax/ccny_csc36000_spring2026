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

PROJECT_ROOT = __file__.rsplit("/", 1)[0]
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


FOLLOWER_ROLE_NAME = "FOLLOWER"
CANDIDATE_ROLE_NAME = "CANDIDATE"
LEADER_ROLE_NAME = "LEADER"


class ReplicaStateMachine:
    """
    A compact Raft implementation tuned for the course test contract.

    The comments are intentionally dense and explicit because the assignment
    asks for code that students can read line-by-line while learning how
    elections, log matching, commit, and recovery interact.
    """

    def __init__(self, replica_id: int, listen_address: str, all_replica_addresses: list[str], start_in_disabled_mode: bool = False):
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

    def start_background_threads(self) -> None:
        if self.disabled_mode_enabled:
            return
        threading.Thread(target=self._run_election_timer_loop, name=f"replica-{self.replica_id}-election", daemon=True).start()
        threading.Thread(target=self._run_heartbeat_loop, name=f"replica-{self.replica_id}-heartbeat", daemon=True).start()

    def stop_background_threads(self) -> None:
        self.stop_requested.set()
        with self.state_lock:
            self.state_changed.notify_all()
        self.transport_pool.close_all()

    def build_status_response(self) -> replica_admin_pb2.StatusResponse:
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
        with self.state_lock:
            if self._raft_participation_is_paused_locked():
                return RequestVoteReply(term=self.current_term, vote_granted=False)

            if request_message.term < self.current_term:
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
                return RequestVoteReply(term=self.current_term, vote_granted=True)

            return RequestVoteReply(term=self.current_term, vote_granted=False)

    def handle_append_entries(self, request_message: AppendEntriesMessage) -> AppendEntriesReply:
        with self.state_lock:
            if self._raft_participation_is_paused_locked():
                return AppendEntriesReply(term=self.current_term, success=False, match_index=self._last_log_index_locked())

            if request_message.term < self.current_term:
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
                return AppendEntriesReply(
                    term=self.current_term,
                    success=False,
                    match_index=self._last_log_index_locked(),
                )

            if self.log_entries[request_message.prev_log_index].term != request_message.prev_log_term:
                conflicting_index = request_message.prev_log_index
                while conflicting_index > 0 and self.log_entries[conflicting_index].term == self.log_entries[request_message.prev_log_index].term:
                    conflicting_index -= 1
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
        client_message_key = (request_message.client_id, request_message.client_msg_id)

        with self.state_lock:
            if self._raft_participation_is_paused_locked():
                return ClientWriteReply(
                    accepted=False,
                    term=self.current_term,
                    leader_hint=self.known_leader_address,
                    seq=0,
                    error_message="replica is administratively stopped",
                )

            if self.role_name != LEADER_ROLE_NAME:
                return ClientWriteReply(
                    accepted=False,
                    term=self.current_term,
                    leader_hint=self.known_leader_address,
                    seq=0,
                    error_message="write must go to the current leader",
                )

            existing_committed_sequence = self.committed_sequence_by_client_message_key.get(client_message_key)
            if existing_committed_sequence is not None:
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

        write_committed = self._replicate_until_committed(target_log_index=pending_sequence, timeout_seconds=3.5)

        with self.state_lock:
            committed_sequence = self.committed_sequence_by_client_message_key.get(client_message_key)
            if write_committed and committed_sequence is not None:
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
        with self.state_lock:
            if self._raft_participation_is_paused_locked():
                return ReadConversationReply(
                    accepted=False,
                    term=self.current_term,
                    leader_hint=self.known_leader_address,
                    served_by=[],
                    events=[],
                    error_message="replica is administratively stopped",
                )

            if self.last_applied_index < self.commit_index:
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
        if request_message.paused:
            self.enter_disabled_mode()
        else:
            self.exit_disabled_mode()
        return ReplicaAvailabilityReply(acknowledged=True)

    def _run_election_timer_loop(self) -> None:
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
                    continue

                with self.state_lock:
                    if peer_reply.term > self.current_term:
                        self._become_follower_locked(new_term=peer_reply.term, leader_address="")
                        break

                    if self.role_name != CANDIDATE_ROLE_NAME or self.current_term != candidate_term:
                        break

                    if peer_reply.vote_granted:
                        granted_votes += 1

                    if granted_votes >= majority_vote_count:
                        self._become_leader_locked()
                        break

    def _run_heartbeat_loop(self) -> None:
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
                    else:
                        current_next_index = self.next_index_by_peer_address.get(peer_address, self._last_log_index_locked() + 1)
                        self.next_index_by_peer_address[peer_address] = max(1, min(current_next_index - 1, peer_reply.match_index + 1))

            with self.state_lock:
                self._advance_commit_index_locked()
        finally:
            self.replication_lock.release()

    def _advance_commit_index_locked(self) -> None:
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
        local_last_log_term = self._last_log_term_locked()
        if candidate_last_log_term != local_last_log_term:
            return candidate_last_log_term > local_last_log_term
        return candidate_last_log_index >= self._last_log_index_locked()

    def _become_follower_locked(self, new_term: int, leader_address: str) -> None:
        self.current_term = new_term
        self.role_name = FOLLOWER_ROLE_NAME
        self.voted_for_replica_id = None
        self.known_leader_address = leader_address
        self._reset_election_deadline_locked()
        self.state_changed.notify_all()

    def _become_leader_locked(self) -> None:
        self.role_name = LEADER_ROLE_NAME
        self.known_leader_address = self.listen_address
        next_index = self._last_log_index_locked() + 1
        self.next_index_by_peer_address = {peer_address: next_index for peer_address in self.peer_addresses}
        self.match_index_by_peer_address = {peer_address: 0 for peer_address in self.peer_addresses}
        self.state_changed.notify_all()

    def _last_log_index_locked(self) -> int:
        return self.log_entries[-1].index

    def _last_log_term_locked(self) -> int:
        return self.log_entries[-1].term

    def _majority_count(self) -> int:
        return (len(self.all_replica_addresses) // 2) + 1

    def _reset_election_deadline_locked(self) -> None:
        self.election_deadline_monotonic = time.monotonic() + random.uniform(1.4, 2.3)

    def _raft_participation_is_paused_locked(self) -> bool:
        return self.disabled_mode_enabled or time.monotonic() < self.rejoin_ready_time_monotonic

    def enter_disabled_mode(self) -> None:
        with self.state_lock:
            self.disabled_mode_enabled = True
            self.role_name = FOLLOWER_ROLE_NAME
            self.known_leader_address = ""
            self.state_changed.notify_all()

    def exit_disabled_mode(self) -> None:
        with self.state_lock:
            self.disabled_mode_enabled = False
            self.role_name = FOLLOWER_ROLE_NAME
            self.known_leader_address = ""
            self.rejoin_ready_time_monotonic = time.monotonic() + 1.0
            self._reset_election_deadline_locked()
            self.state_changed.notify_all()

    @staticmethod
    def _role_name_to_proto_enum(role_name: str) -> int:
        if role_name == FOLLOWER_ROLE_NAME:
            return replica_admin_pb2.FOLLOWER
        if role_name == CANDIDATE_ROLE_NAME:
            return replica_admin_pb2.CANDIDATE
        return replica_admin_pb2.LEADER


class ReplicaAdminService(replica_admin_pb2_grpc.ReplicaAdminServicer):
    def __init__(self, replica_state_machine: ReplicaStateMachine):
        self.replica_state_machine = replica_state_machine

    def Status(self, request, context):
        return self.replica_state_machine.build_status_response()


def build_argument_parser() -> argparse.ArgumentParser:
    argument_parser = argparse.ArgumentParser(description="Run one Raft replica that also exposes the ReplicaAdmin Status RPC.")
    argument_parser.add_argument("--host", default="127.0.0.1")
    argument_parser.add_argument("--port", type=int, required=True)
    argument_parser.add_argument("--replica-count", type=int, default=DEFAULT_REPLICA_COUNT)
    argument_parser.add_argument("--replica-start-port", type=int, default=DEFAULT_REPLICA_START_PORT)
    argument_parser.add_argument("--disabled", action="store_true", help="Run a placeholder replica that only answers Status.")
    return argument_parser


def main() -> int:
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
        pass
    finally:
        replica_state_machine.stop_background_threads()
        grpc_server.stop(grace=None)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

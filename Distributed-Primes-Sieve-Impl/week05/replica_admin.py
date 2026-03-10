import sys
import os
import time
import grpc
from concurrent import futures

# Ensures Python finds the 'generated' folder
sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))

import replica_admin_pb2
import replica_admin_pb2_grpc

class ReplicaAdmin(replica_admin_pb2_grpc.ReplicaAdminServicer):
    def __init__(self, node_id, port):
        self.node_id = node_id
        self.port = port
        
        # --- RAFT STATE (To be implemented) ---
        self.role = "FOLLOWER"  # Roles: FOLLOWER, CANDIDATE, LEADER
        self.current_term = 0
        self.commit_index = 0
        self.leader_hint = ""   # host:port of the current leader
        
        # --- CHAT DATA (To be implemented) ---
        self.messages = {}      # Key: "user_a:user_b", Value: List of messages
        self.seen_messages = set() # For deduplication: (client_id, client_msg_id)

    # how the test suite checks node status
    def Status(self, request, context):
        return replica_admin_pb2.StatusResponse(
            id=self.node_id,
            role=replica_admin_pb2.Role.Value(self.role),
            term=self.current_term,
            leader_hint=self.leader_hint,
            commit_index=self.commit_index
        )

    # --- TODO: Implement RequestVote and AppendEntries RPCs ---

def serve(node_id, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replica_node = ReplicaAdmin(node_id, port)
    
    replica_admin_pb2_grpc.add_ReplicaAdminServicer_to_server(replica_node, server)
    
    # Scripts expect the node to listen on 127.0.0.1
    server.add_insecure_port(f'127.0.0.1:{port}')
    server.start()
    print(f"Replica {node_id} started on 127.0.0.1:{port}")
    server.wait_for_termination()

if __name__ == "__main__":
    # Parsing arguments sent by run_cluster.py
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--id", type=int) # Note: run_cluster.py might pass id differently
    
    args, unknown = parser.parse_known_args()
    
    # Extract ID from log name or arguments if available
    # For now, we use a placeholder if the script doesn't pass it directly
    node_id = args.id if args.id else 1 
    
    serve(node_id, args.port)
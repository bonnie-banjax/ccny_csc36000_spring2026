import os
import json
import grpc
from concurrent.futures import ThreadPoolExecutor
from generated import replica_admin_pb2_grpc
from generated import direct_gateway_pb2, direct_gateway_pb2_grpc

dedup_store = {}

def load_replica_stubs(cluster_json_path=None):
    """Load replica addresses from cluster.json and create gRPC stubs."""
    if cluster_json_path is None:
        cluster_json_path = os.path.join(os.path.dirname(__file__), '.runtime/cluster.json')
        cluster_json_path = os.path.abspath(cluster_json_path)
    with open(cluster_json_path, 'r') as f:
        cluster = json.load(f)
    stubs = []
    for replica in cluster.get('replicas', []):
        addr = replica['addr']
        channel = grpc.insecure_channel(addr)
        stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
        stubs.append({'id': replica['id'], 'addr': addr, 'stub': stub})
    return stubs

class DirectGatewayServicer(direct_gateway_pb2_grpc.DirectGatewayServicer):
    def __init__(self, replica_stubs):
        self.replica_stubs = replica_stubs

    def SendDirect(self, request, context):
        key = (request.client_id, request.client_msg_id)
        if key in dedup_store:
            return direct_gateway_pb2.SendDirectResponse(seq=dedup_store[key])
        leader_stub = self.replica_stubs[0]['stub']
        seq = 1  # Placeholder
        dedup_store[key] = seq
        return direct_gateway_pb2.SendDirectResponse(seq=seq)

    def GetConversationHistory(self, request, context):
        replica_stub = self.replica_stubs[0]['stub']
        events = []
        served_by = [self.replica_stubs[0]['addr']]
        return direct_gateway_pb2.GetConversationHistoryResponse(events=events, served_by=served_by)

def serve():
    stubs = load_replica_stubs()
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    direct_gateway_pb2_grpc.add_DirectGatewayServicer_to_server(
        DirectGatewayServicer(stubs), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("DirectGateway gRPC server started on port 50051")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()

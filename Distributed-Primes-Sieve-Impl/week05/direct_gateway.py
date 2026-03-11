from concurrent import futures
from generated import direct_gateway_pb2, direct_gateway_pb2_grpc

# In-memory deduplication store: {(client_id, client_msg_id): seq}
dedup_store = {}

class DirectGatewayServicer(direct_gateway_pb2_grpc.DirectGatewayServicer):
	def __init__(self, replica_stubs):
		self.replica_stubs = replica_stubs

	def SendDirect(self, request, context):
		# Deduplication check
		key = (request.client_id, request.client_msg_id)
		if key in dedup_store:
			return direct_gateway_pb2.SendDirectResponse(seq=dedup_store[key])
		# Forward to leader (for now, pick first stub)
		leader_stub = self.replica_stubs[0]['stub']
		# You would call the replica's AppendEntry or similar here
		# For now, just fake a response
		seq = 1  # Placeholder
		dedup_store[key] = seq
		return direct_gateway_pb2.SendDirectResponse(seq=seq)

	def GetConversationHistory(self, request, context):
		# Forward to a replica (for now, pick first stub)
		replica_stub = self.replica_stubs[0]['stub']
		# You would call the replica's GetConversationHistory or similar here
		# For now, just fake a response
		events = []
		served_by = [self.replica_stubs[0]['addr']]
		return direct_gateway_pb2.GetConversationHistoryResponse(events=events, served_by=served_by)

def serve():
	stubs = load_replica_stubs()
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	direct_gateway_pb2_grpc.add_DirectGatewayServicer_to_server(
		DirectGatewayServicer(stubs), server)
	server.add_insecure_port('[::]:50051')
	server.start()
	print("DirectGateway gRPC server started on port 50051")
	server.wait_for_termination()

if __name__ == "__main__":
	serve()
import os
import json
import grpc
from generated import replica_admin_pb2_grpc

def load_replica_stubs(cluster_json_path=None):
	"""Load replica addresses from cluster.json and create gRPC stubs."""
	if cluster_json_path is None:
		# Default path relative to this file
		cluster_json_path = os.path.join(os.path.dirname(__file__), '../.runtime/cluster.json')
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

# Example usage:
if __name__ == "__main__":
	stubs = load_replica_stubs()
	print("Loaded replica stubs:")
	for s in stubs:
		print(f"Replica {s['id']} at {s['addr']}")
import grpc
from concurrent import futures
import time

import generated.direct_gateway_pb2 as direct_gateway_pb2
import generated.direct_gateway_pb2_grpc as direct_gateway_pb2_grpc

class DirectGatewayServicer(direct_gateway_pb2_grpc.DirectGatewayServicer):
	def SendDirect(self, request, context):
		# TODO: Implement logic to send a direct message via Raft cluster
		response = direct_gateway_pb2.SendDirectResponse(seq=1)  # Placeholder
		return response

	def GetConversationHistory(self, request, context):
		# TODO: Implement logic to fetch conversation history from Raft cluster
		response = direct_gateway_pb2.GetConversationHistoryResponse(
			events=[], served_by=[]
		)  # Placeholder
		return response

def serve():
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	direct_gateway_pb2_grpc.add_DirectGatewayServicer_to_server(DirectGatewayServicer(), server)
	server.add_insecure_port('[::]:50051')
	server.start()
	print("Gateway server started on port 50051.")
	try:
		while True:
			time.sleep(86400)
	except KeyboardInterrupt:
		server.stop(0)

if __name__ == '__main__':
	serve()

import grpc
import primes_pb2
import primes_pb2_grpc

def run_test():
    # Connect to the worker
    channel = grpc.insecure_channel('localhost:50052')
    stub = primes_pb2_grpc.WorkerServiceStub(channel)

    print("--- Testing Health RPC ---")
    try:
        health_resp = stub.Health(primes_pb2.HealthCheckRequest())
        print(f"Status: {health_resp.status}")
    except grpc.RpcError as e:
        print(f"Health check failed: {e.code()}")

    print("\n--- Testing ComputeRange RPC ---")
    try:
        request = primes_pb2.ComputeRequest(
            low=1,
            high=100,
            mode=primes_pb2.COUNT,
            secondary_exec=primes_pb2.THREADS,
            chunk=50,
            secondary_workers=2
        )
        response = stub.ComputeRange(request)
        print(f"Total Primes: {response.total_primes}")
        print(f"Max Prime: {response.max_prime}")
        print(f"Elapsed Time: {response.elapsed_seconds:.4f}s")
    except grpc.RpcError as e:
        print(f"ComputeRange failed: {e.details()}")

if __name__ == '__main__':
    run_test()
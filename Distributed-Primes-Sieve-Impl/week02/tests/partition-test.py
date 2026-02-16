
import primes_pb2
from secondary_node import compute_partitioned, _work_chunk, iter_ranges_generator
from primes_in_range import primes_sieve, worker_task
import json

def test_compute():
# 1. Construct the synthetic request
  request = primes_pb2.ComputeRequest(
    low=2,
    high=1000,
    chunk=100,
    secondary_exec=primes_pb2.PROCESSES,
    mode=primes_pb2.LIST,
    max_return_primes=50
  )

  try:
    response = compute_partitioned(request)


    log_data = {
        "total_primes": response.total_primes,
        "max_prime": response.max_prime,
        "elapsed_seconds": response.elapsed_seconds,
        "primes": list(response.primes),
        "primes_truncated": response.primes_truncated,
        "nodes_used": response.nodes_used,
        "per_node": [
            {
                "total_primes": pnr.total_primes,
                "max_prime": pnr.max_prime,
                # Convert Protobuf repeated field/tuple to list
                "slice": [pnr.slice.low, pnr.slice.high]
            } for pnr in response.per_node
        ]
    }



    # 4. Write serialized log
    with open("compute_results.json", "w") as f:
      json.dump(log_data, f, indent=4)

    print("Test successful. Results logged to compute_results.json")

  except Exception:
    print(f"Test failed!\n{traceback.format_exc()}")

if __name__ == "__main__":
  test_compute()
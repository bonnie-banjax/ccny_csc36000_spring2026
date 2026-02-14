**Task 3 & 4 (Implementation of coodinator service and updating the CLI distributed mode) on February 11 at 14:57.**

+ Task 3:

  ***Files modified***: week02/core/primary_node.py,
               week02/core/secondary_node.py,
               week02/primes.proto
  
  * Implemented **CoordinatorService** gRPC server in primary_node.py:
      - Handles node registration, node listing, and distributed prime computation.
  * Implemented **WorkerService** gRPC server in secondary_node.py:
      - Handles compute requests and health checks.
  * Updated **primes.proto** to define CoordinatorService and WorkerService RPCs and messages.

+ Task 4:

  ***File modified***: week02/core/primes_cli.py
  * Added distributed mode to CLI:
     + Allows users to run distributed prime computation via command line.
     + Connects to CoordinatorService and manages distributed requests.

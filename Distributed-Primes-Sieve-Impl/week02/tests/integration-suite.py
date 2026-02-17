
# BEGIN awful hack
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LOBBY_DIR = BASE_DIR.parent # I know I could have done .parent.parent above

DIR_CORE = LOBBY_DIR / "core"
DIR_INFRA = LOBBY_DIR / "infra"
DIR_TESTS = LOBBY_DIR / "tests"

IS_LOG_DIR = LOBBY_DIR / "tests" / "system_test.log"

# 3. Ensure the directory exists so you don't get 'FileNotFound'
IS_LOG_DIR.parent.mkdir(parents=True, exist_ok=True)

LOG_FILE = str(LOBBY_DIR / "tests" / "system_test.log")

PRIMARY_NODE = str(LOBBY_DIR / "core" / "primary_node.py")
WORKER_NODE_GRPC = str(LOBBY_DIR / "core" / "secondary_node.py")
PRIMES_CLI = str(LOBBY_DIR/ "core" / "primes_cli.py") #defunct
CLIENT_CLI = str(LOBBY_DIR/ "core" / "client_cli.py")


if str(DIR_CORE) not in sys.path:
    sys.path.insert(0, str(DIR_CORE))
if str(DIR_INFRA) not in sys.path:
    sys.path.insert(0, str(DIR_INFRA))
if str(DIR_TESTS) not in sys.path:
    sys.path.insert(0, str(DIR_TESTS))

# END awful hack


import subprocess
import time
import os
import grpc
import primes_pb2
import primes_pb2_grpc
from datetime import datetime

PRIMARY_GRPC = "127.0.0.1:50051"
PYTHON_BIN = "python3"

# Timing
POST_PRIMARY_SLEEP = 2
POST_WORKERS_SLEEP = 3


def dir_tracker():
  with open(LOG_FILE, "a") as f:
    f.write( # this will probably be removed soon
      f"\nBASE_DIR: {BASE_DIR}\n"
      f"\nLOBBY_DIR: {LOBBY_DIR}\n"
      f"\nDIR_CORE: {DIR_CORE}\n"
      f"\nDIR_INFRA: {DIR_INFRA}\n"
      f"\nDIR_TESTS: {DIR_TESTS}\n"
      f"\nLOG_FILE: {LOG_FILE}\n"
      f"\nPRIMARY_NODE: {PRIMARY_NODE}\n"
      f"\nWORKER_NODE_GRPC: {WORKER_NODE_GRPC}\n"
      f"\nPRIMES_CLI: {PRIMES_CLI}\n"
    )

def create_unique_logfile_handle():
  return  str(LOBBY_DIR / "tests" / f"run-{datetime.now().strftime("%Y-%m-%d%H-%M-%S")}.log")

# BEGIN Parametric Test Suite
def log_separator(handle, label: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    border = "=" * 20
    handle.write(f"\n\n{border} {label} [{timestamp}] {border}\n")
    handle.flush()

def start_coordinator(log_handle):
    print(f"Starting Coordinator...")
    return subprocess.Popen(
        [PYTHON_BIN, "-u", PRIMARY_NODE, "--grpc-port", "50051"],
        stdout=log_handle, stderr=log_handle
    )

def start_workers(count, log_handle):
    workers = []
    for i in range(1, count + 1):
        port = 50060 + i
        p = subprocess.Popen(
            [PYTHON_BIN, "-u", WORKER_NODE_GRPC, "--port", str(port),
             "--coordinator", PRIMARY_GRPC, "--node-id", f"Worker-{port}"],
            stdout=log_handle, stderr=log_handle
        )
        workers.append(p)
    return workers

def register_ghost(target_grpc):
    channel = grpc.insecure_channel(target_grpc)
    stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
    ghost_request = primes_pb2.RegisterRequest(
        node_id="Ghost-Node", host="127.0.0.1", port=9998
    )
    stub.RegisterNode(ghost_request)

def register_ghosts(count, target_grpc):
    channel = grpc.insecure_channel(target_grpc)
    stub = primes_pb2_grpc.CoordinatorServiceStub(channel)

    for i in range(1, count + 1):
      port = 9900 + i
      ghost_request = primes_pb2.RegisterRequest(
        node_id=f"Ghost-Node-{i}", host="127.0.0.1", port=port
      )
      stub.RegisterNode(ghost_request)

def run_cli_test(test_label, extra_args, log_handle):
    """
    test_label: "List Test", "Count Test", etc.
    extra_args: list of flags like ["--mode", "list", "--high", "5000"]
    """
    log_separator(log_handle, f"CLI START: {test_label}")

    base_cmd = [PYTHON_BIN, CLIENT_CLI, "--exec", "distributed", "--primary", PRIMARY_GRPC]
    full_cmd = base_cmd + extra_args

    print(f"Running scenario: {test_label}...")
    subprocess.run(full_cmd, stdout=log_handle, stderr=log_handle)

    log_separator(log_handle, f"CLI END: {test_label}")
# A list of test variations

SCENARIOS = [
    {"name": "Standard Count", "args": ["--low", "2", "--high", "1000000", "--mode", "count"]},
    {"name": "Primes List",   "args": ["--low", "2", "--high", "1000", "--mode", "list"]},
    {"name": "Small Chunks",  "args": ["--low", "2", "--high", "100000", "--chunk", "1000"]},
]

def parametric_test_suite(num_workers=3, num_ghosts=1):
    log_file_handle = open(create_unique_logfile_handle(), "a") # Open in Append mode (was just LOGFILE)
    log_separator(log_file_handle, "NEW TEST SESSION STARTED")

    primary = None
    workers = []

    try:
        # CHUNK 1: Infra
        primary = start_coordinator(log_file_handle)
        time.sleep(POST_PRIMARY_SLEEP)

        # CHUNK 2: Workers (Parametrized count)
        workers = start_workers(num_workers, log_file_handle)
        time.sleep(POST_WORKERS_SLEEP)

        # CHUNK 3: Optional Ghost (Toggle this as needed)
        # register_ghost(PRIMARY_GRPC)
        register_ghosts(num_ghosts, PRIMARY_GRPC)

        # CHUNK 4: Scenarios (Loop through your list)
        for scenario in SCENARIOS:
            run_cli_test(scenario["name"], scenario["args"], log_file_handle)

    finally:
        print("Cleaning up...")
        if primary: primary.terminate()
        for w in workers: w.terminate()
        log_file_handle.close()
# END

# BEGIN old big test

def simple_log_separator(msg):
    with open(LOG_FILE, "a") as f:
        f.write(f"\n{'='*20} {msg} {'='*20}\n")

def og_test_monolith():
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    log_file_handle = open(LOG_FILE, "a")

    try:
        # 1. Start Coordinator (Primary)
        print(f"Starting Coordinator at {PRIMARY_GRPC}...")
        primary_proc = subprocess.Popen(
            [PYTHON_BIN, "-u", PRIMARY_NODE, "--grpc-port", "50051"],
            stdout=log_file_handle, stderr=log_file_handle
        )
        time.sleep(POST_PRIMARY_SLEEP)

        # 2. Start Secondaries (Workers)
        # We pass the coordinator address directly as expected by your new main()
        workers = []
        print("Starting gRPC Secondary Nodes...")
        for i in range(1, 4):
            port = 50060 + i
            p = subprocess.Popen(
                [
                    PYTHON_BIN, "-u", WORKER_NODE_GRPC,
                    "--port", str(port),
                    "--coordinator", PRIMARY_GRPC,
                    "--node-id", f"Worker-{port}"
                ],
                stdout=log_file_handle, stderr=log_file_handle
            )
            workers.append(p)

        time.sleep(POST_WORKERS_SLEEP)

        # 3. Register a Fake Node using gRPC
        # This replaces the urllib/json "Ghost-Node" logic
        print("Registering Ghost-Node via gRPC (Testing Partial Failure)...")
        channel = grpc.insecure_channel(PRIMARY_GRPC)
        stub = primes_pb2_grpc.CoordinatorServiceStub(channel)

        ghost_request = primes_pb2.RegisterRequest(
            node_id="Ghost-Node",
            host="127.0.0.1",
            port=9998  # Dead port
        )
        try:
            stub.RegisterNode(ghost_request)
        except grpc.RpcError as e:
            print(f"Ghost registration reported: {e.code()}")

        print("Initiating Distributed Compute Request...")
        simple_log_separator("CLI OUTPUT START")
        cli_cmd = [
            PYTHON_BIN, CLIENT_CLI,
            "--low", "0",
            "--high", "1000000",
            "--exec", "distributed",
            "--primary", PRIMARY_GRPC, # Change to --coordinator
            "--mode", "count"
        ]

        subprocess.run(cli_cmd, stdout=log_file_handle, stderr=log_file_handle)
        time.sleep(POST_WORKERS_SLEEP)
        simple_log_separator("REQUEST TWO")
        cli_cmd = [
            PYTHON_BIN, CLIENT_CLI,
            "--low", "0",
            "--high", "1000000",
            "--exec", "distributed",
            "--primary", PRIMARY_GRPC, # Change to --coordinator
            "--mode", "list"
        ]
        subprocess.run(cli_cmd, stdout=log_file_handle, stderr=log_file_handle)
        simple_log_separator("CLI OUTPUT END")


        print(f"Test complete. Check '{LOG_FILE}' for results.")

    except Exception as e:
        print(f"System Test encountered error: {e}")
    finally:
        print("Cleaning up cluster...")
        primary_proc.terminate()
        for w in workers:
            w.terminate()
        log_file_handle.close()

# END old big test

def main():
  # og_test_monolith()
  # time.sleep(2)
  parametric_test_suite();

if __name__ == "__main__":
    main()
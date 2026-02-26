
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

# A list of test variations
SCENARIOS = [
  {"name": "Standard Count", "args": ["--low", "2", "--high", "1000000", "--mode", "count"]},
  {"name": "Primes List",   "args": ["--low", "2", "--high", "1000", "--mode", "list"]},
  {"name": "Small Chunks",  "args": ["--low", "2", "--high", "100000", "--chunk", "1000"]},
]

# BEGIN port helper
import socket

def get_free_port() -> int:
  """
  Asks the OS for a free ephemeral port, then releases it.
  Returns the port number.
  """
  # AF_INET = IPv4, SOCK_STREAM = TCP
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # Bind to port 0 tells the OS to assign a random free port
    s.bind(('', 0))
    # Ensure the OS has actually allocated the resource
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    return s.getsockname()[1] # getsockname returns (address, port)
# END

# BEGIN log business
def create_unique_logfile_handle():
  return  str(LOBBY_DIR / "logs" / f"run-{datetime.now().strftime("%Y-%m-%d%H-%M-%S")}.log")

def log_separator(handle, label: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    border = "=" * 20
    handle.write(f"\n\n{border} {label} [{timestamp}] {border}\n")
    handle.flush()
# END

# BEGIN Parametric Test Suite

def start_coordinator(primary_port, log_handle):
    print(f"Starting Coordinator...")
    return subprocess.Popen(
        [PYTHON_BIN, "-u", PRIMARY_NODE, "--grpc-port", str(primary_port)],
        stdout=log_handle, stderr=log_handle
    )

def start_workers(count, log_handle, coordinator_address):
    workers = []
    for i in range(count):
        # Dynamically grab a port for this specific worker
        port = get_free_port()

        p = subprocess.Popen(
            [PYTHON_BIN, "-u", WORKER_NODE_GRPC,
             "--port", str(port),
             "--coordinator", coordinator_address,
             "--node-id", f"Worker-{port}"],
            stdout=log_handle, stderr=log_handle
        )
        workers.append(p)
    return workers

def register_ghosts(count, target_grpc):
    channel = grpc.insecure_channel(target_grpc)
    stub = primes_pb2_grpc.CoordinatorServiceStub(channel)

    for i in range(count):
      dead_port = get_free_port()  # Guaranteed available, but we won't listen on it
      ghost_request = primes_pb2.RegisterRequest(
        node_id=f"Ghost-Node-{i}", host="127.0.0.1", port=dead_port
      )
      stub.RegisterNode(ghost_request)

def run_cli_test(test_label, extra_args, log_handle, prim_grpc):
    """
    test_label: "List Test", "Count Test", etc.
    extra_args: list of flags like ["--mode", "list", "--high", "5000"]
    """
    log_separator(log_handle, f"CLI START: {test_label}")

    base_cmd = [PYTHON_BIN, CLIENT_CLI, "--exec", "distributed", "--primary", prim_grpc]
    full_cmd = base_cmd + extra_args

    print(f"Running scenario: {test_label}...")
    subprocess.run(full_cmd, stdout=log_handle, stderr=log_handle)

    log_separator(log_handle, f"CLI END: {test_label}")

def parametric_test_suite(log_file_handle, num_workers=3, num_ghosts=1):
    # log_file_handle = open(create_unique_logfile_handle(), "a") # Open in Append mode (was just LOGFILE)
    log_separator(log_file_handle, "NEW TEST SESSION STARTED")

    primary = None
    workers = []

    primary_port = get_free_port()
    dynamic_primary_addr = f"127.0.0.1:{primary_port}"

    try:
        # CHUNK 1: Infra
        primary = start_coordinator(primary_port, log_file_handle)
        time.sleep(POST_PRIMARY_SLEEP)

        # CHUNK 2: Workers (Parametrized count)
        workers = start_workers(num_workers, log_file_handle, dynamic_primary_addr)
        time.sleep(POST_WORKERS_SLEEP)

        # CHUNK 3: Optional Ghost (Toggle this as needed)
        register_ghosts(num_ghosts, dynamic_primary_addr)

        # CHUNK 4: Scenarios (Loop through your list)
        for scenario in SCENARIOS:
            run_cli_test(scenario["name"], scenario["args"], log_file_handle, dynamic_primary_addr)

    finally:
        print("Cleaning up...")
        if primary: primary.terminate()
        for w in workers: w.terminate()
        # log_file_handle.close()
def parametric_test_suite(log_file_handle, num_workers=3, num_ghosts=1):
    # log_file_handle = open(create_unique_logfile_handle(), "a") # Open in Append mode (was just LOGFILE)
    log_separator(log_file_handle, "NEW TEST SESSION STARTED")

    primary = None
    workers = []

    primary_port = get_free_port()
    dynamic_primary_addr = f"127.0.0.1:{primary_port}"

    try:
        # CHUNK 1: Infra
        primary = start_coordinator(primary_port, log_file_handle)
        time.sleep(POST_PRIMARY_SLEEP)

        # CHUNK 2: Workers (Parametrized count)
        workers = start_workers(num_workers, log_file_handle, dynamic_primary_addr)
        time.sleep(POST_WORKERS_SLEEP)

        # CHUNK 3: Optional Ghost (Toggle this as needed)
        register_ghosts(num_ghosts, dynamic_primary_addr)

        # CHUNK 4: Scenarios (Loop through your list)
        for scenario in SCENARIOS:
            run_cli_test(scenario["name"], scenario["args"], log_file_handle, dynamic_primary_addr)

    finally:
        print("Cleaning up...")
        if primary: primary.terminate()
        for w in workers: w.terminate()
        # log_file_handle.close()

# END

# BEGIN glue rand test
import random

def run_randomized_glue_suite(log_handle, iterations=10):
    log_separator(log_handle, f"STARTING {iterations} RANDOMIZED GLUE TESTS")

    for i in range(1, iterations + 1):
        # 1. Randomize parameters
        low = random.randint(2, 500)
        high = random.randint(low + 100, 2000)
        chunk = random.randint(10, 100)
        mode = random.choice(["count", "list"])
        strategy = random.choice(["threads", "processes"])

        # 2. Build the command
        test_args = [
            PYTHON_BIN, CLIENT_CLI,
            "--low", str(low),
            "--high", str(high),
            "--mode", mode,
            "--exec", strategy,
            "--chunk", str(chunk)
        ]

        # 3. Print a "Reproduction String" for debugging
        repro_cmd = " ".join(test_args)
        log_handle.write(f"\n[Test {i}/10] Executing: {repro_cmd}\n")
        log_handle.flush() # Ensure it writes immediately in case of crash

        # 4. Execute
        result = subprocess.run(
            test_args,
            stdout=log_handle,
            stderr=log_handle,
            text=True
        )

        if result.returncode != 0:
            log_handle.write(f"!!! TEST {i} FAILED with exit code {result.returncode} !!!\n")
        else:
            log_handle.write(f"--- TEST {i} PASSED ---\n")

    log_separator(log_handle, "RANDOMIZED GLUE TESTS COMPLETE")
# END


def main():
  log_file_handle = open(create_unique_logfile_handle(), "a")
  try:
    parametric_test_suite(log_file_handle);
    time.sleep(2)
    run_randomized_glue_suite(log_file_handle)
  finally:
    log_file_handle.close()

if __name__ == "__main__":
    main()
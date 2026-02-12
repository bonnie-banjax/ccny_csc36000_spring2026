import subprocess
import time
import urllib.request
import json
import os

# Configuration
LOG_FILE = "tests/system_test.log"
PRIMARY_URL = "http://127.0.0.1:9200"
PYTHON_BIN = "python3"  # Adjust to 'python' if on Windows

def log_separator(msg):
    with open(LOG_FILE, "a") as f:
        f.write(f"\n{'='*20} {msg} {'='*20}\n")

def main():
    # Clear previous logs
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    log_file_handle = open(LOG_FILE, "a")

    try:
        # 1. Start Primary
        print("Starting Primary Node...")
        primary_proc = subprocess.Popen(
            [PYTHON_BIN, "-u", "core/primary_node.py", "--port", "9200"],
            stdout=log_file_handle, stderr=log_file_handle
        )
        time.sleep(2) # Allow boot time

        # 2. Start Two Secondaries
        print("Starting Secondary Nodes...")
        sec1 = subprocess.Popen(
            [PYTHON_BIN, "-u", "core/secondary_node.py", "--port", "9101", "--primary", PRIMARY_URL, "--node-id", "Alpha"],
            stdout=log_file_handle, stderr=log_file_handle
        )
        sec2 = subprocess.Popen(
            [PYTHON_BIN, "-u", "core/secondary_node.py", "--port", "9102", "--primary", PRIMARY_URL, "--node-id", "Beta"],
            stdout=log_file_handle, stderr=log_file_handle
        )
        time.sleep(3) # Allow registration time

        # 3. Register a Fake Node
        print("Registering Fake Node (to test partial failure logic)...")
        fake_payload = {
            "node_id": "Ghost-Node",
            "host": "127.0.0.1",
            "port": 9999, # Port where nothing is listening
            "cpu_count": 1
        }
        req = urllib.request.Request(
            f"{PRIMARY_URL}/register",
            data=json.dumps(fake_payload).encode(),
            headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req)

        # 4. Initiate Primes CLI Distributed Request
        print("Initiating Distributed Compute Request...")
        log_separator("CLI OUTPUT START")
        cli_cmd = [
            PYTHON_BIN, "core/primes_cli.py",
            "--low", "0",
            "--high", "1000000",
            "--exec", "distributed",
            "--primary", PRIMARY_URL,
            "--time",
            "--mode", "count"
        ]

        # We run this synchronously so we can wait for the result
        subprocess.run(cli_cmd, stdout=log_file_handle, stderr=log_file_handle)
        log_separator("CLI OUTPUT END")

        print(f"Test complete. Check '{LOG_FILE}' for results.")

    except Exception as e:
        print(f"Orchestrator encountered error: {e}")
    finally:
        # Cleanup
        print("Shutting down nodes...")
        primary_proc.terminate()
        sec1.terminate()
        sec2.terminate()
        log_file_handle.close()

if __name__ == "__main__":
    main()
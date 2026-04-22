import subprocess
import sys

# The list of commands extracted from your test plan
commands = [
    "python scripts/run_cluster.py",
    # End to end testing
    "python apps/inventory_client.py create-item test-item-1 5",
    "python apps/inventory_client.py reserve test-item-1 r1 2",
    "python apps/inventory_client.py get test-item-1",
    "python apps/inventory_client.py release test-item-1 r1",
    "python apps/inventory_client.py get test-item-1",
    # Over-allocation failure test
    "python apps/inventory_client.py create-item test-item-2 1",
    "python apps/inventory_client.py reserve test-item-2 r1 1",
    "python apps/inventory_client.py reserve test-item-2 r2 1", # Expected fail
    "python apps/inventory_client.py get test-item-2",
    # Invalid release failure test
    "python apps/inventory_client.py create-item test-item-3 2",
    "python apps/inventory_client.py reserve test-item-3 r1 1",
    "python apps/inventory_client.py release test-item-3 r2",    # Expected fail
    "python apps/inventory_client.py get test-item-3",
    # Persistence test
    "python apps/inventory_client.py create-item test-item-4 3",
    "python apps/inventory_client.py reserve test-item-4 r1 1",
    "python scripts/stop_cluster.py",
    "python scripts/run_cluster.py",
    "python apps/inventory_client.py get test-item-4",
    # Final cleanup
    "python scripts/stop_cluster.py"
]

def run_tests():
    for cmd in commands:
        print(f"\n{'-'*60}")
        print(f"EXECUTING: {cmd}")
        print(f"{'-'*60}")

        # We use shell=True to mimic the exact terminal environment
        # text=True ensures we get string output instead of bytes
        process = subprocess.run(
            cmd,
            shell=True,
            capture_output=False,
            text=True
        )

        # Note: We don't check for 'returncode != 0' because your tests
        # specifically include commands that are expected to fail (ValueError).
        # This keeps the script running regardless of the command's success.

if __name__ == "__main__":
    try:
        run_tests()
    except KeyboardInterrupt:
        print("\nTests interrupted by user.")
        subprocess.run("python scripts/stop_cluster.py", shell=True)
        sys.exit(1)

# NOTE this needs to be run from the project's root directory for now

# BEGIN ###

import subprocess
import shutil
import sys
import json
from pathlib import Path


def get_project_root(): # BEGIN ###
    current = Path(__file__).resolve().parent
    for parent in [current] + list(current.parents):
        # print(f"Checking directory: {parent}")
        if (parent / ".git").is_dir():
            # print(f"parent is: {parent}")
            return parent
    print("Error: Can't find project root via  .git directory.")
    sys.exit(1)
# END   ###

def get_engine(): # BEGIN ###
    for tool in ["podman", "docker"]:
        if shutil.which(tool):
            return tool
    print("Error: No container engine found."); sys.exit(1)
# END   ###

def main(): # BEGIN ###
    root = get_project_root()
    engine = get_engine()
    infra_dir = Path(__file__).resolve().parent
    compose_tool = f"{engine}-compose"

    print(f"Starting containers via {engine}...")

    print(f"Executing from: {root}")
    subprocess.run(["ls", "-a"], cwd=root)

    up_cmd = [
      compose_tool,
      "-f", str(infra_dir / "compose.yaml"),
      "up", "-d", "--build"
    ]
    subprocess.run(up_cmd, cwd=root, check=True)

    ps_cmd = [
        compose_tool,
        "-f", str(infra_dir / "compose.yaml"),
        "ps", "--format", "json"
    ]
    result = subprocess.run(ps_cmd, cwd=root, capture_output=True, text=True, check=True)

    print(f"Debug result {result}")
# BEGIN

    print("Locating container ID...")
    find_id_cmd = [
        engine, "ps", "--filter", "label=com.docker.compose.service=grpc-app",
        "--format", "{{.ID}}", "--latest"
    ]
    result = subprocess.run(find_id_cmd, capture_output=True, text=True, check=True)
    container_id = result.stdout.strip()

    if not container_id:
        find_id_cmd = [engine, "ps", "--filter", "ancestor=localhost/infra_dev-env:latest", "--format", "{{.ID}}", "--latest"]
        result = subprocess.run(find_id_cmd, capture_output=True, text=True, check=True)
        container_id = result.stdout.strip()

    if not container_id:
        print("Error: Could not find the running container.")
        sys.exit(1)

# END

    print(f"Container active: {container_id}")

    # BEGIN ###
    # setup_commands = [
    #     "pip install -r requirements.txt",
    #     "python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. src/primes.proto"
    # ]
    #
    # for cmd in setup_commands:
    #     print(f"Running: {cmd}")
    #     subprocess.run([engine, "exec", container_id, "sh", "-c", cmd], check=True)
    # END   ###

# END ###

if __name__ == "__main__":
    main()

# END   ###
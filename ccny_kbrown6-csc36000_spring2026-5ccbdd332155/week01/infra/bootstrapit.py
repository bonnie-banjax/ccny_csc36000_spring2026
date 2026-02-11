
# BEGIN ###

import subprocess
import shutil
import sys
import json
from pathlib import Path


def get_project_root(): # BEGIN ###
    current = Path(__file__).resolve().parent
    for parent in [current] + list(current.parents):
        if (parent / ".git").is_dir():
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
    up_cmd = [
        compose_tool, "--project-directory", str(root),
        "-f", str(infra_dir / "compose.yaml"),
        "up", "-d", "--build"
    ]
    subprocess.run(up_cmd, check=True)

    ps_cmd = [
        compose_tool, "--project-directory", str(root),
        "-f", str(infra_dir / "compose.yaml"),
        "ps", "--format", "json"
    ]
    result = subprocess.run(ps_cmd, capture_output=True, text=True)

    try:
        container_info = json.loads(result.stdout)
        if isinstance(container_info, list):
            container_id = container_info[0]["ID"]
        else:
            container_id = container_info["ID"]
    except Exception:
        # Fallback for older versions: use the service name directly
        container_id = f"{root.name}_grpc-app_1".lower().replace("-", "")

    print(f"Container active: {container_id}")


    setup_commands = [
        "pip install -r requirements.txt",
        "python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. src/primes.proto"
    ]

    for cmd in setup_commands:
        print(f"Running: {cmd}")
        subprocess.run([engine, "exec", container_id, "sh", "-c", cmd], check=True)
# END ###

if __name__ == "__main__":
    main()

# END   ###

# NOTE this needs to be run from the project's root directory for now

# BEGIN ###

import os
import sys
import time
import inspect
import argparse
import subprocess
from pathlib import Path

################################################################################

def get_project_root(): # BEGIN ###
    current = Path(__file__).resolve().parent
    for parent in [current] + list(current.parents):
        if (parent / ".git").is_dir():
            return parent
    print("Error: Can't find project root via  .git directory.")
    return current # Fallback
# END ###

# BEGIN ###
def log_inventory(root, inventory, log_name="inventory.log"):
    with open(root / log_name, "w") as f:
        f.write(f"PROJECT ROOT: {root}\n")
        f.write("-" * 40 + "\n")

        leaf_idx = 0
        for depth, level in enumerate(inventory):
            f.write(f"DEPTH {depth}:\n")
            for path in level:
                # Calculate path relative to root for clarity
                rel_path = path.relative_to(root)
                is_leaf = path.is_file()

                marker = f"[{leaf_idx}] (FILE)" if is_leaf else "(DIR )"
                f.write(f"  {marker} {rel_path}\n")

                if is_leaf:
                    leaf_idx += 1
            f.write("\n")

def get_terminal_leaves(inventory):
    leaves = []
    for level in inventory:
        for path in level:
            if path.is_file():
                leaves.append(path)
    # Now we have an indexed list of every file in the project
    return {i: path for i, path in enumerate(leaves)}

def get_reflexive_inventory():
    # --- PHASE 1: DISCOVERY ---
    caller_file = inspect.getfile(inspect.currentframe())
    script_path = Path(caller_file).resolve()

    root = None
    for parent in [script_path.parent] + list(script_path.parents):
        if (parent / ".git").is_dir():
            root = parent
            break

    if not root:
        raise RuntimeError("Hermetic failure: Could not find .git root anchor.")

    # --- PHASE 2: INVENTORY ---
    inventory = []
    queue = [root]

    while queue:
        level_contents = []
        next_queue = []

        for current_dir in queue:
            for item in current_dir.iterdir():
                # RULE: Ignore hidden things, EXCEPT we want to see the .git
                # folder itself at the root level, but NEVER go inside it.
                if item.name.startswith('.') and item.name != '.git':
                    continue

                level_contents.append(item)

                # THE GATEKEEPER:
                # Only recurse if it's a directory AND not the .git folder
                if item.is_dir() and item.name != '.git':
                    next_queue.append(item)

        if level_contents:
            inventory.append(level_contents)
        queue = next_queue

    return root, script_path, inventory
# END ###

def get_engine(): # BEGIN ###
    for tool in ["podman", "docker"]:
        if shutil.which(tool):
            return tool
    print("Error: No container engine found."); sys.exit(1)
# END   ###

################################################################################


# BEGIN
def find_infra_dir_in_inventory(inventory):
    """Scans the inventory levels to find the directory named 'infra'."""
    for level in inventory:
        for path in level:
            if path.is_dir() and path.name == "infra":
                return path
    return None
# END

# BEGIN
def link_infra_to_root(root, infra_dir):
    """
    Creates a symbolic link at the project root pointing to the
    actual infrastructure directory.
    """
    link_path = root / "infra"

    # Safety: Remove existing link or file if it exists to avoid FileExistsError
    if link_path.exists() or link_path.is_symlink():
        link_path.unlink()

    # Create the symlink: link_path -> infra_dir
    # Note: target is the first argument in os.symlink, but the caller in pathlib
    link_path.symlink_to(infra_dir, target_is_directory=True)

    print(f"Symlink created: {link_path} -> {infra_dir}")
    return link_path
# END

# BEGIN
import shutil

def migrate_infra_to_root(root, infra_dir):
    """
    Physically moves the Containerfile and compose.yaml to the root
    to simplify the context for Podman-Compose.
    """
    src_container = infra_dir / "Containerfile"
    src_compose = infra_dir / "compose.yaml"

    dst_container = root / "Containerfile"
    dst_compose = root / "compose.yaml"

    # Store for cleanup
    migrated_paths = []

    if src_container.exists():
        shutil.copy2(src_container, dst_container)
        migrated_paths.append(dst_container)

    if src_compose.exists():
        shutil.copy2(src_compose, dst_compose)
        migrated_paths.append(dst_compose)

    return migrated_paths

def cleanup_infra_migration(migrated_paths):
    """
    Removes the temporary infrastructure files from the root
    to keep the workspace hermetic and clean.
    """
    print("Cleaning up temporary infrastructure files...")
    for path in migrated_paths:
        try:
            path.unlink(missing_ok=True)
            print(f"  Removed: {path.name}")
        except Exception as e:
            print(f"  Warning: Could not remove {path.name}: {e}")
# END

# BEGIN ###
def synthesize_infra_list(infra_dir: Path):
    """Ensures the environment definitions exist on disk."""
    infra_dir.mkdir(exist_ok=True)

    container_list = [
        "FROM ghcr.io/void-linux/void-glibc-busybox",
        "RUN xbps-install -Su -y && xbps-install -y python3 python3-pip",
        "ENV PYTHONDONTWRITEBYTECODE=1",
        "ENV PIP_BREAK_SYSTEM_PACKAGES=1",
        "WORKDIR /app"
    ]

    compose_list = [
        "services:",
        "  primary:",
        "    build:",
        "      context: ${PROJECT_ROOT}",
        "      dockerfile: infra/Containerfile",
        "    container_name: primes_primary",
        "    volumes:",
        "      - ${PROJECT_ROOT}:/app:Z",
        "    working_dir: /app",
        "    environment:",
        "      - PROJECT_ROOT=${PROJECT_ROOT}",
        "    ports:",
        "      - \"9200:9200\"",
        "      - \"9201:9201\"",
        "    command: [\"tail\", \"-f\", \"/dev/null\"]"
    ]

    c_file = infra_dir / "Containerfile"
    y_file = infra_dir / "compose.yaml"

    container_content = "\n".join(container_list[:5]) + "\n"

    compose_content = "\n".join(compose_list[:14]) + "\n"

    if not c_file.exists():
        print(f"Synthesizing {c_file}...")
        c_file.write_text(container_content)

    if not y_file.exists():
        print(f"Synthesizing {y_file}...")
        y_file.write_text(compose_content)
# END ###

# BEGIN
def synthesize_infra(infra_dir: Path):
    """Ensures the environment definitions exist on disk."""
    infra_dir.mkdir(exist_ok=True)

    container_content = """FROM ghcr.io/void-linux/void-glibc-busybox
RUN xbps-install -Su -y && xbps-install -y python3 python3-pip
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_BREAK_SYSTEM_PACKAGES=1
WORKDIR /app
"""

    compose_content = """services:
  primary:
    build:
      context: ${PROJECT_ROOT}
      dockerfile: infra/Containerfile
    container_name: primes_primary
    volumes:
      - ${PROJECT_ROOT}:/app:Z
    working_dir: /app
    environment:
      - PROJECT_ROOT=${PROJECT_ROOT}
    ports:
      - "9200:9200"
      - "9201:9201"
    command: ["tail", "-f", "/dev/null"]
"""

    c_file = infra_dir / "Containerfile"
    y_file = infra_dir / "compose.yaml"

    if not c_file.exists():
        print(f"Synthesizing {c_file}...")
        c_file.write_text(container_content)

    if not y_file.exists():
        print(f"Synthesizing {y_file}...")
        y_file.write_text(compose_content)
# END

# BEGIN
def synthesize_infra_relative(infra_dir, root):
    # Calculate the relative path from the project root to the Containerfile
    # This ensures Podman-compose always finds it, regardless of nesting.
    relative_containerfile = (infra_dir / "Containerfile").relative_to(root)

    container_content = """FROM ghcr.io/void-linux/void-glibc-busybox
RUN xbps-install -Su -y && xbps-install -y python3 python3-pip
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_BREAK_SYSTEM_PACKAGES=1
WORKDIR /app
"""

    compose_content = f"""services:
  primary:
    build:
      context: ${{PROJECT_ROOT}}
      dockerfile: {relative_containerfile}
    container_name: primes_primary
    volumes:
      - ${{PROJECT_ROOT}}:/app:Z
    working_dir: /app
    ...
"""

    c_file = infra_dir / "Containerfile"
    y_file = infra_dir / "compose.yaml"

    if not c_file.exists():
        print(f"Synthesizing {c_file}...")
        c_file.write_text(container_content)

    if not y_file.exists():
        print(f"Synthesizing {y_file}...")
        y_file.write_text(compose_content)

# END

################################################################################
# BEGIN
def run_podman_logic(root, worker_count):
    # Since we created a symlink [root]/infra -> [nested]/infra
    # the file is now visible at root/infra/compose.yaml

    up_cmd = [
        "podman-compose",
        "-f", "infra/compose.yaml", # Path relative to 'root'
        "-p", "snake_pit",
        "up", "-d", "--build",
        "--scale", f"worker={worker_count}"
    ]

    print(f"Executing: {' '.join(up_cmd)} at {root}")

    # We MUST run this from the root where the symlink exists
    subprocess.run(up_cmd, cwd=root, check=True)

# END
################################################################################
def bootstrap_with_source_capture():
    # 1. Identify the file containing this specific function
    # This works even if bootstrapit.py is imported or run as a script
    this_file_path = Path(inspect.getfile(inspect.currentframe())).resolve()

    # 2. Read the entire source text into memory
    # We use utf-8 to ensure it handles all characters correctly
    try:
        source_text = this_file_path.read_text(encoding='utf-8')
    except Exception as e:
        source_text = f"Error capturing source: {e}"

    # 3. Store it in a variable (now available in memory)
    # This is a raw string containing the exact bytes of bootstrapit.py
    captured_source = source_text

    # --- Verification / Audit ---
    print(f"Captured {len(captured_source)} characters from {this_file_path.name}")

    # You can now use this variable to hash the script,
    # send it to a container, or verify its own integrity.
    return captured_source
    # To get a 'bytes' object instead of a 'str'
    # raw_bytes = this_file_path.read_bytes()

def dig_snake_pit(worker_count: int): # BEGIN ###
    SNAKE_PIT = "snake_pit"
    STRAPPED_BOOT = bootstrap_with_source_capture()


    caller_file = inspect.getfile(inspect.currentframe())
    script_path = Path(caller_file).resolve()

    # BEGIN 2. Identify Project Root and Inventory
    root, _, inventory = get_reflexive_inventory()
    infra_dir = find_infra_dir_in_inventory(inventory)
    print(f"\n\nInfra Dir Found: {infra_dir} \n\n")
    # infra_dir = root / "infra"
    # log_dir = root / "logs"
    sym_infra = link_infra_to_root(root,infra_dir)
    # 3. Log the Inventory for Verification
    # This creates inventory.log in the root for you to audit.
    log_inventory(root, inventory)
    # END

    # BEGIN Infra Files Back Synthesis
    # 4. Synthesize Infrastructure if missing
    # This ensures the Factory (the script) can recreate its Environment.
    # synthesize_infra_list(infra_dir)
    # synthesize_infra_relative(infra_dir, root)
    # END

    # 5. Set the Environment Variable for Compose
    # We pass the absolute root found by the treewalker to the YAML.
    os.environ["PROJECT_ROOT"] = str(root)

    print(f"--- Starting Hermetic Environment [Reflexive Mode] ---")
    print(f"Root: {root}")

    # BEGIN pod spinning logic
    try:
        # 6. Spin up the Pod
        # We use the -p project name to ensure container IDs are deterministic.
        subprocess.run([
            "podman-compose",
            "-f", str(sym_infra / "compose.yaml"),
            "-p", f"{SNAKE_PIT}",
            "up", "-d", "--build"
        ], cwd=root, check=True)

        time.sleep(2)
        run_podman_logic(root, worker_count)
        time.sleep(2)

        exec_cmd = [
            "podman", "exec", "-i", f"{SNAKE_PIT}",
            "python3", "-", "--dry-run"  # The '-' tells Python to read from stdin
        ]

        # 3. Execute and "Pipe" the string in
        print("Teleporting bootstrapper source to container...")
        subprocess.run(
            exec_cmd,
            input=STRAPPED_BOOT,  # This 'feeds' the string to the container's python
            text=True,            # Ensures it treats input as a string, not bytes
            check=True
        )

        # 7. Trigger Internal Setup
        # Only happens inside the hermetic container; host stays clean.
        print("Verifying Filesystem Transition...")
        test_cmd = "import os; print(f'Container sees {len(os.listdir(\".\"))} items in /app')"
        subprocess.run([
          "podman", "exec", f"{SNAKE_PIT}",
          "python3", "-c", test_cmd
        ], check=True)
    # END
    except subprocess.CalledProcessError as e:
        print(f"Bootstrap failed: {e}")
        sys.exit(1)

    print("\n--- System Live ---")
    print(f"Inventory logged to: {root}/inventory.log")
    print("Run nodes via: podman exec -it snake_pit python3 core/primary_node.py")

# END ###
################################################################################

if __name__ == "__main__": # BEGIN
    parser = argparse.ArgumentParser(description="Hermetic Environment Bootstrapper")
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Number of worker nodes")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and log the project inventory without starting containers."
    )


    args = parser.parse_args()

    if args.dry_run:
        print("Running in Dry-Run mode...")
        root, script_path, inventory = get_reflexive_inventory()
        log_inventory(root, inventory)
        print(f"Inventory synthesized and logged to {root}/inventory.log")
        print("No container actions performed.")
    else:
        # Full execution
        dig_snake_pit(args.workers)

# END                          ###
################################################################################
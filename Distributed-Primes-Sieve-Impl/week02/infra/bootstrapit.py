
# NOTE this needs to be run from the project's root directory for now

# BEGIN ###

import os
import sys
import time
import venv
import shutil
import inspect
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

################################################################################

# NOTE Global Boostrapper Instance State

STATE = {
  "project_root"     : None,
  "container_engine" : None,
  "global_inventory" : None
}

PROJECT_ROOT     = None
CONTAINER_ENGINE = None
GLOBAL_INVENTORY = None

class BootConfig:
  BOOT_STACK       = []
  BOOT_STATE       = {}
  PROJECT_ROOT     = None
  CONTAINER_ENGINE = None
  GLOBAL_INVENTORY = None

# Convert class attributes to dictionary
# config_dict = {k: v for k, v in Config.__dict__.items() if not k.startswith("__")}


################################################################################

def get_engine(): # BEGIN ###
  for tool in ["podman", "docker"]:
    if shutil.which(tool):
      return tool
  print("Error: No container engine found."); return "none"
# END   ###

def get_project_root(state: dict): # BEGIN ###

  # if proj_root is not None:
  #   return project_root

  if state.PROJECT_ROOT is not None:
    return state.PROJECT_ROOT

  if os.environ.get("IS_BOOTSTRAPPED"): # ("IS_BOOTSTRAPPED") == "1"
    return Path.cwd() # return Path("/app")


  result = _project_root_heuristic(".git")
  if result is not None:
    return result


  if (Path(__file__).is_relative_to(Path.cwd())):
    return Path.cwd()



  return False
# END ###

def _project_root_heuristic(target): # BEGIN ###

  activation_location = Path(os.getcwd())
  if (activation_location / target).is_dir():
    return activation_location

  # root = None
  # for parent in [script_path.parent] + list(script_path.parents):
  #   if (parent / ".git").is_dir():
  #     root = parent
  #     break

  current = Path(__file__).resolve().parent
  for parent in [current] + list(current.parents):
    if (parent / target).is_dir():
      return parent;

  print(f"Error: Can't find project root via {target} directory.")
  return None # Fallback
# END ###


# BEGIN ###
def log_inventory(root, inventory, log_place, log_name="inventory"):

  log_place.mkdir(parents=True, exist_ok=True)
  timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  log_file = log_place / f"{log_name}_{timestamp}.log"

  with open(log_file, "w") as f:
    f.write(f"PROJECT ROOT: {root}\n")
    f.write("-" * 40 + "\n")

    leaf_idx = 0;
    for depth, level in enumerate(inventory):
      f.write(f"DEPTH {depth}:\n")
      for path in level:
        # Calculate path relative to root for clarity
        rel_path = path.relative_to(root)
        is_leaf = path.is_file()

        marker = f"[{leaf_idx}] (FILE)" if is_leaf else "(DIR )"
        f.write(f" {marker} {rel_path}\n")

        if is_leaf:
          leaf_idx += 1
      f.write("\n")


def _reflexive_inventory(root): # BEGIN

  if not root: raise RuntimeError("Root not provided")

  ignore_hidden = True # tempory boolean

  inventory = []
  queue = [os.path.abspath(root)]   # Ensure root is an absolute string path


  while queue:
    level_contents = []
    next_queue = []

    for current_dir in queue:
      try:    # os.listdir returns names, not full paths
        items = os.listdir(current_dir)
      except OSError:
        continue
      for name in items:
        full_path = os.path.join(current_dir, name)
        if name.startswith('.') and ignore_hidden:
          continue
        level_contents.append(full_path)
        if os.path.isdir(full_path):
          next_queue.append(full_path)
    if level_contents:
      inventory.append(level_contents)
    queue = next_queue

  return inventory
# END


def _reflexive_inventory_git(root, inventory=None): # BEGIN

  if not root: raise RuntimeError("Root not provided")

  inventory = [] if inventory is None else inventory
  queue = [os.path.abspath(root)]   # Ensure root is an absolute string path

  while queue:
    level_contents = []
    next_queue = []

    for current_dir in queue:
      try:    # os.listdir returns names, not full paths
        items = os.listdir(current_dir)
      except OSError:
        continue
      for name in items:
        full_path = os.path.join(current_dir, name)
        if name.startswith('.') and name != '.git':
          continue
        level_contents.append(full_path)
        if os.path.isdir(full_path) and name != '.git':
          next_queue.append(full_path)
    if level_contents:
      inventory.append(level_contents)
    queue = next_queue

  return root, inventory
# END

def get_reflexive_inventory(): # BEGIN

  root = PROJECT_ROOT # get_project_root()

  if not root:
    raise RuntimeError("Hermetic failure: Could not find .git root anchor.")

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
        if item.is_dir() and item.name != '.git':
          next_queue.append(item)

    if level_contents:
      inventory.append(level_contents)
    queue = next_queue

  return root, inventory
# END ###

def get_terminal_leaves(inventory):
    leaves = []
    for level in inventory:
        for path in level:
            if path.is_file():
                leaves.append(path)
    # Now we have an indexed list of every file in the project
    return {i: path for i, path in enumerate(leaves)}
# END

################################################################################

# BEGIN
def pivot_to_venv_adaptive(project_root: Path):
  venv_dir = project_root / ".venv"
  # Logic for finding the python binary (Windows vs Linux)
  python_bin = venv_dir / ("Scripts" if os.name == "nt" else "bin") / "python"

  # Check: Are we already running from the venv?
  if sys.prefix == str(venv_dir):
    return # Already pivoted

  # If venv doesn't exist, create it
  if not venv_dir.exists():
    print(f"Creating fresh venv at {venv_dir}...")
    venv.create(venv_dir, with_pip=True)
    # Optional: Install requirements immediately before pivoting
    # subprocess.run([str(python_bin), "-m", "pip", "install", "-r", "requirements.txt"], check=True)

  print("Pivoting to virtual environment...")
  # Replace current process with the venv python process
  # sys.argv[0] is the script name, sys.argv[1:] are the flags
  os.execv(str(python_bin), [str(python_bin)] + sys.argv)


def pivot_to_fresh_venv(project_root: Path):
  venv_dir = project_root / ".venv"
  python_bin = venv_dir / ("Scripts" if os.name == "nt" else "bin") / "python"

  # Critical: Check if we are in the venv we are about to delete!
  if sys.prefix == str(venv_dir):
    return

  if venv_dir.exists():
    print("Nuking existing venv for a fresh start...")
    shutil.rmtree(venv_dir)

  print("Provisioning fresh hermetic venv...")
  venv.create(venv_dir, with_pip=True)

  # Re-execute
  os.execv(str(python_bin), [str(python_bin)] + sys.argv)
# END

################################################################################


# BEGIN ###
def synthesize_infra_list(destination: Path):
    """Ensures the environment definitions exist on disk."""
    destination.mkdir(exist_ok=True)

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

    c_file = destination / "Containerfile"
    y_file = destination / "compose.yaml"

    container_content = "\n".join(container_list[:5]) + "\n"

    compose_content = "\n".join(compose_list[:14]) + "\n"

    if not c_file.exists():
        print(f"Synthesizing {c_file}...")
        c_file.write_text(container_content)

    if not y_file.exists():
        print(f"Synthesizing {y_file}...")
        y_file.write_text(compose_content)
# END ###

# BEGIN ### find_relative_path
def find_relative_path(filename: str, inventory_dict: dict, root: Path) -> str:
    """Finds a file in the inventory and returns its path relative to root."""
    for path in inventory_dict.values():
        if path.name == filename:
            # .relative_to(root) turns '/home/user/repo/tests/test.py' into 'tests/test.py'
            return str(path.relative_to(root));
    raise FileNotFoundError(f"Could not find {filename} in project inventory.");


def find_relative_path(filename, inventory_dict, root):
  """Finds a file in the inventory and returns its path relative to root."""
  # Ensure root is absolute for consistent relative path calculation
  root_abs = os.path.abspath(root)

  for path in inventory_dict.values():
    if os.path.basename(path) == filename:
      return os.path.relpath(path, root_abs)

  raise FileNotFoundError(f"Could not find {filename} in project inventory.")
# END ###

# BEGIN
def find_dir_in_inventory(targ_dir, inventory):
  for level in inventory:
    for path in level:
      if path.is_dir() and path.name == targ_dir:
        return path
  return None

def mass_dir_to_sys_path(inventory):
  for level in inventory:
    for path in level:
      if path.is_dir() and str(path) not in sys.path:
        print(f"\nAdding \n{path}\n{str(path)}\n to sys.path\n")
        sys.path.insert(0, str(path))
  return None

def focused_dir_to_sys_path(inventory):
  for level in inventory:
    for path in level:
      if path.is_dir() and not path.name.startswith('.') and path.name != "__pycache__":
        if str(path) not in sys.path:
          sys.path.insert(0, str(path))
  return None

def env_make(root, inventory):
  extra_paths = ":".join([str(p) for level in inventory for p in level if p.is_dir()])
  env = os.environ.copy()
  env["PYTHONPATH"] = extra_paths + (":" + env.get("PYTHONPATH", "") if env.get("PYTHONPATH") else "")
  return env
# END


def create_isolated_sandbox(src="/app_host", dst="/app_sandbox"):
  if os.path.exists(dst):
    shutil.rmtree(dst) # Clear old runs

  # copytree replicates the whole directory tree
  shutil.copytree(src, dst, symlinks=True, ignore=shutil.ignore_patterns('.git', '__pycache__'))

  # Now switch the working directory to the sandbox
  os.chdir(dst)
  print(f"Sandbox created and active at: {dst}")

################################################################################

# BEGIN
def spawn_service(root, compose_path, pod_name, service_name, count):

  up_cmd = [
    "podman-compose",
    "--profile", service_name,
    "-f", compose_path,
    "-p", pod_name,
    "up", "-d", "--build",
    "--scale", f"{service_name}={count}",
  ]

  print(f"Executing: {' '.join(up_cmd)} at {root}")

  subprocess.run(up_cmd, cwd=root, check=True)
# END

# BEGIN
def spawn_service_complex(root, compose_path, pod_name, service_name, profiles=None, count=1):
  # Initialize the command base
  up_cmd = ["podman-compose"]

  # Add profiles if provided (e.g., ["workers_only"])
  if profiles:
    for profile in profiles:
      up_cmd.extend(["--profile", profile])

  # Add the rest of the standard arguments
  up_cmd.extend([
    "-f", compose_path, # Path relative to 'root'
    "-p", pod_name,
    "up", "-d", "--build",
    "--scale", f"{service_name}={count}",
  ])

  print(f"Executing: {' '.join(up_cmd)} at {root}")
  subprocess.run(up_cmd, cwd=root, check=True)
# END
################################################################################

def dry_run(): # BEGIN ###
  print("Running in Dry-Run mode...")
  root, inventory = get_reflexive_inventory()
  logs_dir = find_dir_in_inventory("logs", inventory)
  log_inventory(root, inventory, logs_dir)
  print(f"Inventory dry ran and logged to {logs_dir}/inventory.log")
  print("No container actions performed.")
# END ###

def run_all_tests(): # BEGIN ###

  caller_file = inspect.getfile(inspect.currentframe())
  script_path = Path(caller_file).resolve()

  root, inventory = get_reflexive_inventory()
  file_map = get_terminal_leaves(inventory)

  logs_dir = find_dir_in_inventory("logs", inventory)
  log_inventory(root, inventory, logs_dir)

  req_filename = "requirements.txt"
  relative_req_path = find_relative_path(req_filename, file_map, root)

  suite_filename = "integration-suite.py"
  relative_suite_path = find_relative_path(suite_filename, file_map, root)

  env = env_make(root, inventory)

  result1 = subprocess.run(
    [sys.executable, "-m", "pip", "install", "-r", relative_req_path],
    check=True
  )
  result2 = subprocess.run(
    [sys.executable, "-u", relative_suite_path],
    env=env,
    check=True,      # Raise exception if script fails
    text=True        # Handle output as strings instead of bytes
  )
  return True
# END ###

################################################################################

def known_commands():
  SNAKE_PIT = False # placeholder
  TARGET = False # placeholder
  FLAG = False # placeholder
  relative_compose_path = False # placeholder
  relative_suite_path  = False # placeholder

  cmd_spin_primary = [
    "podman-compose",
    "-f", relative_compose_path,
    "-p", f"{SNAKE_PIT}",
    "up", "-d", "--build",
    "primary"
  ] #cwd=root, check=true
  c2 = [
    "podman", "exec", "-i", f"{TARGET}",
    "python3", "-", f"{FLAG}"  # The '-' tells Python to read from stdin
  ] # text=true, check=true, input=(thing not here)
  c3 = [
    "podman", "exec",
    "-w", "/app",        # Ensure we are in the project root
    f"{SNAKE_PIT}",
    "python3", relative_suite_path
  ] # check=true

  # not totally sure how to turn into commands like above
  print("Run nodes via: podman exec -it snake_pit python3 core/primary_node.py")

  return True

def bootstrap_with_source_capture():
  # 1. Identify the file containing this specific function
  # This works even if bootstrapit.py is imported or run as a script
  this_file_path = Path(inspect.getfile(inspect.currentframe())).resolve()

  # 2. Read the entire source text into memory
  # We use utf-8 to ensure it handles all characters correctly
  # To get a 'bytes' object instead of a 'str' use
  # raw_bytes = this_file_path.read_bytes()
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

# BEGIN
def spoon_feed_source(TARGET, FLAG):

  # SNAKE_PIT = "snake_pit"
  STRAPPED_BOOT = bootstrap_with_source_capture()

  exec_cmd = [
    "podman", "exec", "-i", f"{TARGET}",
    "python3", "-", f"{FLAG}"  # The '-' tells Python to read from stdin
  ]

  # BEGIN 3. Execute and "Pipe" the string in
  print("[2] Teleporting bootstrapper source to container...")
  result = subprocess.run(
    exec_cmd,
    input=STRAPPED_BOOT,  # This 'feeds' the string to the container's python
    text=True,            # Ensures it treats input as a string, not bytes
    check=True
  ) # END

  return result
# END

# BEGIN
def spoon_feed_to_service_type(service_name, flag):
  cmd = [
    "podman", "ps", "--format", "{{.Names}}",
    "--filter", f"label=io.podman.compose.service={service_name}"
  ]
  container_names = subprocess.check_output(cmd, text=True).splitlines()

  for name in container_names:
    print(f"[1] Teleporting to {name}...")
    spoon_feed_source(name, flag)
# END

# BEGIN
def broadcast_source(targets, flag):
  """
  targets: can be a single string 'snake_pit' or a list ['worker_1', 'worker_2']
  """
  source = bootstrap_with_source_capture()

  # Normalize targets to a list
  if isinstance(targets, str):
    # If it's a service name, find its children, otherwise assume it's one container
    targets = [targets]

  for target in targets:
    exec_cmd = ["podman", "exec", "-i", target, "python3", "-", flag]
    subprocess.run(exec_cmd, input=source, text=True, check=True)
# END
################################################################################

def dig_snake_pit(worker_count: int): # BEGIN ###
  SNAKE_PIT = "snake_pit"
  root, inventory = get_reflexive_inventory()
  file_map = get_terminal_leaves(inventory)


  core_dir = find_dir_in_inventory("core", inventory)
  infra_dir = find_dir_in_inventory("infra", inventory)
  tests_dir = find_dir_in_inventory("tests", inventory)
  logs_dir = find_dir_in_inventory("logs", inventory)

  log_inventory(root, inventory, logs_dir)

  # BEGIN root relative
  compose_filename = "compose.yaml"
  relative_compose_path = find_relative_path(compose_filename, file_map, root)

  containerfile_filename = "Containerfile"
  relative_container_filename = find_relative_path(containerfile_filename, file_map, root)
  # END

  os.environ["PROJECT_ROOT"] = str(root)
  os.environ["CONTAINER_FILE"] = str(relative_container_filename)

  print(f"--- Starting Hermetic Environment [Reflexive Mode] ---")

  # BEGIN pod spinning logic
  try:
    # spawn_service(root, relative_compose_path, SNAKE_PIT, "isolated", ["isolated"] 1)

    spawn_service(root, relative_compose_path, SNAKE_PIT, "isolated", 1)
    # spawn_service(root, relative_compose_path, SNAKE_PIT, "primary", 1)
    # spawn_service(root, relative_compose_path, SNAKE_PIT, "worker", worker_count)

    spoon_feed_to_service_type("isolated", "--run-all")
    # spoon_feed_to_service_type("primary", "--run-all")
    # spoon_feed_to_service_type("worker", "--run-all")

    # spoon_feed_to_service_type("isolated", "--dry-run")
    # spoon_feed_to_service_type("primary", "--dry-run")
    # spoon_feed_to_service_type("worker", "--dry-run")

  # END
  except subprocess.CalledProcessError as e:
    print(f"Bootstrap failed: {e}")
    sys.exit(1)

  print("\n--- System Live ---")
  print(f"Inventory logged to: {root}/inventory.log")


# END ###


################################################################################

if __name__ == "__main__": # BEGIN
  parser = argparse.ArgumentParser(description="Hermetic Environment Bootstrapper")
  parser.add_argument(
    "--here",
    action="store_true",
    help="Set activation directory as project root")
  parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Build and log the project inventory without starting containers.")
  parser.add_argument(
    "--run-all",
    action="store_true",
    help="Run the integration test suite")
  parser.add_argument(
    "--workers",
    type=int,
    default=3,
    help="Number of worker nodes")

  args = parser.parse_args()


  if args.here:
    PROJECT_ROOT = Path.cwd()
  else:
    PROJECT_ROOT = get_project_root(BootConfig())

  match (args.dry_run, args.run_all):
    case (True, _):
      dry_run()
    case (_, True):
      run_all_tests()
    case (False, False):
      dig_snake_pit(args.workers)


# END                          ###
################################################################################
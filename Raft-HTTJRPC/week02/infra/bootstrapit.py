
# NOTE this needs to be run from the project's root directory for now

# BEGIN dependencies / imports
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
# END   dependencies / imports

################################################################################

# NOTE Global Boostrapper Instance State

PROJECT_ROOT     = None
CONTAINER_ENGINE = None
GLOBAL_INVENTORY = None

class BootConfig:
  BOOT_STACK       = []
  BOOT_STATE       = {}
  PROJECT_ROOT     = None
  CONTAINER_ENGINE = None
  GLOBAL_INVENTORY = None
# END

################################################################################

def get_engine():
  for tool in ["podman", "docker"]:
    if shutil.which(tool):
      return tool
  print("Error: No container engine found."); return "none"
# END

def get_project_root(state: dict):

  if state.PROJECT_ROOT is not None:
    return state.PROJECT_ROOT

  if os.environ.get("IS_BOOTSTRAPPED"): # ("IS_BOOTSTRAPPED") == "1"
    return Path.cwd()

  result = _project_root_heuristic(".git")
  if result is not None:
    return result

  if (Path(__file__).is_relative_to(Path.cwd())):
    return Path.cwd()

  return False
# END

def _project_root_heuristic(target):

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
# END

def log_inventory(root, inventory, log_place, log_name="inventory"):
  # os.makedirs with exist_ok=True replaces log_place.mkdir(parents=True, ...)
  os.makedirs(log_place, exist_ok=True)

  timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

  # os.path.join replaces the / operator
  log_file_name = f"{log_name}_{timestamp}.log"
  log_file = os.path.join(log_place, log_file_name)

  with open(log_file, "w") as f:
    f.write(f"PROJECT ROOT: {root}\n")
    f.write("-" * 40 + "\n")

    leaf_idx = 0
    for depth, level in enumerate(inventory):
      f.write(f"DEPTH {depth}:\n")
      for path in level:
        # os.path.relpath replaces path.relative_to(root)
        rel_path = os.path.relpath(path, root)

        # os.path.isfile replaces path.is_file()
        is_leaf = os.path.isfile(path)

        marker = f"[{leaf_idx}] (FILE)" if is_leaf else "(DIR )"
        f.write(f" {marker} {rel_path}\n")

        if is_leaf:
          leaf_idx += 1
      f.write("\n")
# END

def _reflexive_inventory(root):

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

def get_terminal_leaves(inventory):
    leaves = []
    for level in inventory:
        for path in level:
            if os.path.isfile(path):
                leaves.append(path)
    # Now we have an indexed list of every file in the project
    return {i: path for i, path in enumerate(leaves)}

# END

################################################################################

def find_relative_path(filename, inventory_dict, root):
  """Finds a file in the inventory and returns its path relative to root."""
  # Ensure root is absolute for consistent relative path calculation
  root_abs = os.path.abspath(root)
  for path in inventory_dict.values():
    if os.path.basename(path) == filename:
      return os.path.relpath(path, root_abs)
  raise FileNotFoundError(f"Could not find {filename} in project inventory.")
# END

def find_dir_in_inventory(targ_dir, inventory):
  for level in inventory:
    for path in level:
      if os.path.isdir(path) and os.path.basename(path) == targ_dir:
        return path
  return None
# END

def mass_dir_to_sys_path(inventory):
  for level in inventory:
    for path in level:
      if path.is_dir() and str(path) not in sys.path:
        print(f"\nAdding \n{path}\n{str(path)}\n to sys.path\n")
        sys.path.insert(0, str(path))
  return None
# END

def focused_dir_to_sys_path(inventory):
  for level in inventory:
    for path in level:
      if path.is_dir() and not path.name.startswith('.') and path.name != "__pycache__":
        if str(path) not in sys.path:
          sys.path.insert(0, str(path))
  return None
# END

def env_make_with_venv_path(root, inventory):
    # 1. Collect all directory paths for PYTHONPATH
    dir_list = [p for level in inventory for p in level if os.path.isdir(p)]
    extra_python_paths = os.pathsep.join(dir_list)

    # 2. Start with a fresh copy of the current environment
    env = os.environ.copy()

    # 3. AUTO-VENV PATH INJECTION
    # Get the bin/Scripts directory of the current Python interpreter
    # This ensures subprocesses stay in the venv we pivoted into.
    current_venv_bin = os.path.dirname(sys.executable)

    existing_path = env.get("PATH", "")
    if current_venv_bin not in existing_path:
        env["PATH"] = f"{current_venv_bin}{os.pathsep}{existing_path}"

    # 4. PYTHONPATH INJECTION
    current_pythonpath = env.get("PYTHONPATH", "")
    if current_pythonpath:
        env["PYTHONPATH"] = f"{extra_python_paths}{os.pathsep}{current_pythonpath}"
    else:
        env["PYTHONPATH"] = extra_python_paths

    return env
# END


################################################################################

# BEGIN Updated Spawn Service (detects compose method)
def get_compose_command():
    """Returns 'podman-compose', 'docker-compose', or 'docker compose'."""
    # 1. Check for podman-compose
    if shutil.which("podman-compose"):
        return ["podman-compose"]

    # 2. Check for modern docker compose (the plugin)
    # We check 'docker' first, then see if 'compose' works
    if shutil.which("docker"):
        try:
            result = subprocess.run(["docker", "compose", "version"],
                                    capture_output=True, text=True)
            if result.returncode == 0:
                return ["docker", "compose"]
        except Exception:
            pass

    # 3. Check for the old docker-compose standalone
    if shutil.which("docker-compose"):
        return ["docker-compose"]

    raise RuntimeError("Neither podman-compose nor docker-compose were found.")

# Updated Spawn Service
def spawn_service(root, compose_path, pod_name, service_name, count):
"""Requires service name to match profile name"""
    cmd_base = get_compose_command() # Detect command

    up_cmd = cmd_base + [
        "--profile", service_name,
        "-f", compose_path,
        "-p", pod_name,
        "up", "-d", "--build",
        "--scale", f"{service_name}={count}",
    ]

    print(f"Executing: {' '.join(up_cmd)} at {root}")
    subprocess.run(up_cmd, cwd=root, check=True)

# Updated Spawn Service (complex)
def spawn_service_complex(root, compose_path, pod_name, service_name, profiles=None, count=1):
"""Service name doesn't have to match profile name"""
    up_cmd = get_compose_command()     # Detect command

    if profiles:
        for profile in profiles:
            up_cmd.extend(["--profile", profile])

    up_cmd.extend([
        "-f", compose_path,
        "-p", pod_name,
        "up", "-d", "--build",
        "--scale", f"{service_name}={count}",
    ])

    print(f"Executing: {' '.join(up_cmd)} at {root}")
    subprocess.run(up_cmd, cwd=root, check=True)
# END   Updated Spawn Service


################################################################################

# BEGIN Condensed Versions of modular bootenv
def ensure_venv(venv_dir):
  if sys.prefix != sys.base_prefix:
    return True
  if not os.path.exists(venv_dir):
    venv.create(venv_dir, with_pip=True)
  if sys.platform == "win32":
    python_exe = os.path.join(venv_dir, "Scripts", "python.exe")
  else:
    python_exe = os.path.join(venv_dir, "bin", "python")
  os.execv(python_exe, [python_exe] + sys.argv)
  return False
# ###
def install_package(package_name):
  original_argv = sys.argv.copy()
  sys.argv = [sys.executable, "install", package_name]
  try:
    runpy.run_module("pip", run_name="__main__")
  except SystemExit:
    pass
  finally:
    sys.argv = original_argv
  return True
# ###
def validate_package(package_name):
  import_name = package_name.replace('-', '_')
  try:
    importlib.invalidate_caches()
    module = importlib.import_module(import_name)
    path = getattr(module, '__file__', 'Unknown Location')
    return True
  except ImportError:
    return False
# ###
def driver(venv_dir, package_name):
  ensure_venv(venv_dir)
  install_package(package_name)
  success = validate_package(package_name)
  if success:
    sys.exit(0) # return True
  else:
    sys.exit(1) # return False
# ###
# END   Condensed Versions of modular bootenv

################################################################################

def dry_run(root):
  print("Running in Dry-Run mode...")
  inventory = _reflexive_inventory(root)
  logs_dir = find_dir_in_inventory("logs", inventory)
  log_inventory(root, inventory, logs_dir)
  print(f"Inventory dry ran and logged to {logs_dir}/inventory.log")
  print("No container actions performed.")
# END

def run_all_tests(root):

  if os.environ.get("IS_BOOTSTRAPPED"):
    pass
  else: ensure_venv(".auto_venv")

  inventory = _reflexive_inventory(root)
  file_map = get_terminal_leaves(inventory)

  req_filename = "requirements.txt"
  relative_req_path = find_relative_path(req_filename, file_map, root)

  suite_filename = "integration-suite.py"
  relative_suite_path = find_relative_path(suite_filename, file_map, root)

  env = env_make_with_venv_path(root, inventory)

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
# END

################################################################################

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
# END

def spoon_feed_to_service_type(service_name, source, args):

  cmd = [
    f"{CONTAINER_ENGINE}", "ps", "--format", "{{.Names}}",
    "--filter", f"label=io.podman.compose.service={service_name}"
  ]
  container_names = subprocess.check_output(cmd, text=True).splitlines()

  for name in container_names:
    print(f"[1] Teleporting to {name}...")

    exec_cmd = [
      f"{CONTAINER_ENGINE}", "exec", "-i",
      f"{name}", "python3", "-", f"{args}"
    ]

    result = subprocess.run(
      exec_cmd,
      input=source,  # This 'feeds' the string to the container's python
      text=True,     # Ensures it treats input as a string, not bytes
      check=True
    )
# END

################################################################################

def spin_podman_up(root, worker_count: int):
  inventory = _reflexive_inventory(root)
  file_map = get_terminal_leaves(inventory)

  core_dir  = find_dir_in_inventory("core",  inventory)
  infra_dir = find_dir_in_inventory("infra", inventory)
  tests_dir = find_dir_in_inventory("tests", inventory)
  logs_dir  = find_dir_in_inventory("logs",  inventory)

  # BEGIN root relative
  compose_filename = "compose.yaml"
  relative_compose_path = find_relative_path(compose_filename, file_map, root)

  containerfile_filename = "Containerfile"
  relative_container_filename = find_relative_path(containerfile_filename, file_map, root)
  # END

  os.environ["PROJECT_ROOT"] = str(root)
  os.environ["CONTAINER_FILE"] = str(relative_container_filename)

  print(f"--- Starting Hermetic Environment [Reflexive Mode] ---")

  try:   # BEGIN pod spinning logic
    SNAKE_PIT = "snake_pit"
    SOURCE = bootstrap_with_source_capture()
    active = (0, 1, 0, 0, 1, 0, 0, 0, 0)

    if active[0]: spawn_service(root, relative_compose_path, SNAKE_PIT, "isolated", 1)
    if active[1]: spawn_service(root, relative_compose_path, SNAKE_PIT, "primary",  1)
    if active[2]: spawn_service(root, relative_compose_path, SNAKE_PIT, "worker", worker_count)

    if active[3]: spoon_feed_to_service_type("isolated", SOURCE, "--run-all")
    if active[4]: spoon_feed_to_service_type("primary",  SOURCE, "--run-all")
    if active[5]: spoon_feed_to_service_type("worker",   SOURCE, "--run-all")

    if active[6]: spoon_feed_to_service_type("isolated",SOURCE, "--dry-run")
    if active[7]: spoon_feed_to_service_type("primary", SOURCE, "--dry-run")
    if active[8]: spoon_feed_to_service_type("worker",  SOURCE, "--dry-run")

  # END
  except subprocess.CalledProcessError as e:
    print(f"Bootstrap failed: {e}")
    sys.exit(1)

  print("\n--- System Live ---")
  print(f"Inventory logged to: {root}/inventory.log")


# END

################################################################################

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Hermetic Environment Bootstrapper")
  parser.add_argument(
    "--here",
    action="store_true",
    help="Set activation directory as project root")
  parser.add_argument(
    "--log-inventory",
    action="store_true",
    help="Build and log the project inventory without starting containers.")
  parser.add_argument(
    "--run-all",
    action="store_true",
    help="Run the integration test suite")
  parser.add_argument(
    "--container",
    action="store_true",
    help="Start containerized project")
  parser.add_argument(
    "--workers",
    type=int,
    default=3,
    help="Number of worker nodes")

  args = parser.parse_args()

  CONTAINER_ENGINE = get_engine()
  print(f"Get Engine Result: {CONTAINER_ENGINE}")

  if args.here:
    PROJECT_ROOT = Path.cwd()
  else:
    PROJECT_ROOT = get_project_root(BootConfig())

  if args.log_inventory:
    dry_run(PROJECT_ROOT)

  match (args.run_all, args.container):
    case (True, _):
      run_all_tests(PROJECT_ROOT)
    case (_,True):
      spin_podman_up(PROJECT_ROOT, args.workers)
# END

################################################################################
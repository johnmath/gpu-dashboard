import subprocess
import json
from datetime import datetime
import os
import glob # New import to scan for files
import time # New import to check file age

# --- Configuration ---
script_dir = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(script_dir, 'config.json')

try:
    with open(config_path) as f:
        config = json.load(f)
    # This is *now only* for the hub machine itself
    SERVERS = config['servers']
except FileNotFoundError:
    print(f"FATAL: config.json not found at {config_path}")
    exit(1)

OUTPUT_FILE = os.path.join(script_dir, 'status.json')
INCOMING_DIR = os.path.join(script_dir, 'incoming')
STALE_THRESHOLD_SECONDS = 300 # 5 minutes
# ---------------------


def run_ssh_command(server_address, command):
    """Runs a command on a remote server via SSH and returns the output."""
    try:
        cmd = ["ssh", server_address, command]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=10)
        return result.stdout.strip()
    except Exception as e:
        print(f"Error connecting to {server_address}: {e}")
        return None

def get_username_from_pid(server_address, pids):
    """Gets a map of {pid: username} from a list of PIDs."""
    if not pids: return {}
    pid_str = ",".join(pids)
    command = f"ps -o pid=,user= -p {pid_str}"
    output = run_ssh_command(server_address, command)
    if not output: return {}
    users = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 2: users[parts[0]] = parts[1]
    return users

def fetch_server_stats(server_name, server_address):
    """Fetches all GPU and process stats from a single server."""
    print(f"Querying {server_name}...")
    server_data = {"name": server_name, "gpus": [], "error": None}
    gpu_query = "nvidia-smi --query-gpu=index,memory.used,memory.total,utilization.gpu,uuid --format=csv,noheader,nounits"
    gpu_output = run_ssh_command(server_address, gpu_query)
    
    if gpu_output is None:
        server_data["error"] = "Failed to connect or run nvidia-smi."
        return server_data

    gpus = {}
    for line in gpu_output.splitlines():
        parts = line.split(', ')
        gpus[parts[4]] = {
            "index": int(parts[0]), "mem_used": int(parts[1]),
            "mem_total": int(parts[2]), "util": int(parts[3]),
            "processes": []
        }

    proc_query = "nvidia-smi --query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory --format=csv,noheader,nounits"
    proc_output = run_ssh_command(server_address, proc_query)
    
    pid_to_gpu = {}
    pids_on_server = []
    
    if proc_output:
        for line in proc_output.splitlines():
            parts = line.split(', ')
            gpu_uuid, pid, proc_name, mem_used = parts
            pids_on_server.append(pid)
            pid_to_gpu[pid] = {"uuid": gpu_uuid, "name": proc_name, "mem": int(mem_used)}

    user_map = get_username_from_pid(server_address, pids_on_server)

    for pid, proc_data in pid_to_gpu.items():
        gpu_uuid = proc_data["uuid"]
        if gpu_uuid in gpus:
            gpus[gpu_uuid]["processes"].append({
                "pid": pid, "name": proc_data["name"],
                "user": user_map.get(pid, "unknown"), "mem": proc_data["mem"]
            })
            
    server_data["gpus"] = list(gpus.values())
    return server_data


# --- Main execution ---
all_stats = {
    "servers": [],
    "last_updated": datetime.utcnow().isoformat() + "Z"
}

# 1. Fetch stats for the 'hub' machine (lambda) itself
for name, address in SERVERS.items():
    stats = fetch_server_stats(name, address)
    all_stats["servers"].append(stats)

# 2. Scan the 'incoming' directory for stats from 'spoke' machines
print(f"Scanning for spoke data in {INCOMING_DIR}...")
spoke_files = glob.glob(os.path.join(INCOMING_DIR, "*.json"))
current_time = time.time()

for file_path in spoke_files:
    try:
        # Check if the file is stale (e.g., machine is offline)
        file_mtime = os.path.getmtime(file_path)
        if (current_time - file_mtime) > STALE_THRESHOLD_SECONDS:
            # File is old, report an error
            machine_name = os.path.basename(file_path).split('.')[0]
            print(f"Found stale data for {machine_name}.")
            all_stats["servers"].append({
                "name": machine_name,
                "gpus": [],
                "error": "Data is stale. Machine may be offline."
            })
            continue # Skip to next file
            
        # File is fresh, read it
        with open(file_path) as f:
            spoke_data = json.load(f)
            print(f"Adding data for {spoke_data.get('name')}")
            all_stats["servers"].append(spoke_data)
            
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

# 3. Write the final, combined file
with open(OUTPUT_FILE, 'w') as f:
    json.dump(all_stats, f, indent=2)

print(f"Successfully wrote combined stats to {OUTPUT_FILE}")

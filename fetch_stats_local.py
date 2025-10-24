import subprocess
import json
import os
import socket

# --- Configuration ---
# Get this machine's name to use as the key
# This will get 'machine-2' from 'machine-2.mylab.edu'
SERVER_NAME = socket.gethostname().split('.')[0]
OUTPUT_FILE = "my_stats.json"
# ---------------------

# NOTE: We can't use the SSH functions here because we are local.
# We have to run the commands directly.

def run_local_command(command):
    """Runs a command locally."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=10, shell=True)
        return result.stdout.strip()
    except Exception as e:
        print(f"Error running command {command}: {e}")
        return None

def get_username_from_pid(pids):
    """Gets a map of {pid: username} from a list of PIDs."""
    if not pids: return {}
    pid_str = ",".join(pids)
    command = f"ps -o pid=,user= -p {pid_str}"
    output = run_local_command(command)
    
    if not output: return {}
    users = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 2: users[parts[0]] = parts[1]
    return users

def fetch_server_stats(server_name):
    """Fetches all GPU and process stats from the local machine."""
    print(f"Querying local stats for {server_name}...")
    server_data = {"name": server_name, "gpus": [], "error": None}
    
    gpu_query = "nvidia-smi --query-gpu=index,memory.used,memory.total,utilization.gpu,uuid --format=csv,noheader,nounits"
    gpu_output = run_local_command(gpu_query)
    
    if gpu_output is None:
        server_data["error"] = "Failed to run nvidia-smi."
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
    proc_output = run_local_command(proc_query)
    
    pid_to_gpu = {}
    pids_on_server = []
    
    if proc_output:
        for line in proc_output.splitlines():
            parts = line.split(', ')
            gpu_uuid, pid, proc_name, mem_used = parts
            pids_on_server.append(pid)
            pid_to_gpu[pid] = {"uuid": gpu_uuid, "name": proc_name, "mem": int(mem_used)}

    user_map = get_username_from_pid(pids_on_server)

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
# This just returns the data, doesn't put it in the "all_stats" wrapper
final_data = fetch_server_stats(SERVER_NAME)

with open(OUTPUT_FILE, 'w') as f:
    json.dump(final_data, f, indent=2)

print(f"Successfully wrote local stats to {OUTPUT_FILE}")

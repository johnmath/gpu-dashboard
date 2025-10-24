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
    """Fetches all GPU, CPU, and process stats from the local machine."""
    print(f"Querying local stats for {server_name}...")
    # ADDED cpu_util
    server_data = {"name": server_name, "gpus": [], "cpu_util": 0, "error": None}
    
    # 1. Get CPU stats
    # Gets user + system percent
    cpu_command = "top -bn1 | grep '%Cpu(s)' | awk '{print $2 + $4}'"
    cpu_output = run_local_command(cpu_command)
    try:
        server_data["cpu_util"] = float(cpu_output.strip()) if cpu_output else 0
    except Exception as e:
        print(f"Could not parse CPU stats: {e}")
        server_data["cpu_util"] = 0

    # 2. Get GPU stats
    # ADDED 'name' to the query
    gpu_query = "nvidia-smi --query-gpu=index,memory.used,memory.total,utilization.gpu,uuid,name --format=csv,noheader,nounits"
    gpu_output = run_local_command(gpu_query)
    
    if gpu_output is None:
        server_data["error"] = "Failed to run nvidia-smi."
        return server_data

    gpus = {}
    for line in gpu_output.splitlines():
        # Clean up whitespace and parse
        parts = [p.strip() for p in line.split(',')]
        gpus[parts[4]] = { # uuid is parts[4]
            "index": int(parts[0]), 
            "mem_used": int(parts[1]),
            "mem_total": int(parts[2]), 
            "util": int(parts[3]),
            "name": parts[5], # ADDED
            "processes": []
        }

    # 3. Get Process stats
    # ADDED 'elapsed_time' to the query
    proc_query = "nvidia-smi --query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory,elapsed_time --format=csv,noheader,nounits"
    proc_output = run_local_command(proc_query)
    
    pid_to_gpu = {}
    pids_on_server = []
    
    if proc_output:
        for line in proc_output.splitlines():
            parts = [p.strip() for p in line.split(',')]
            gpu_uuid, pid, proc_name, mem_used = parts[:4]
            pids_on_server.append(pid)
            pid_to_gpu[pid] = {
                "uuid": gpu_uuid, 
                "name": proc_name, 
                "mem": int(mem_used),
                "time": parts[4] # ADDED
            }

    # 4. Get Usernames
    user_map = get_username_from_pid(pids_on_server)

    # 5. Combine all data
    for pid, proc_data in pid_to_gpu.items():
        gpu_uuid = proc_data["uuid"]
        if gpu_uuid in gpus:
            gpus[gpu_uuid]["processes"].append({
                "pid": pid, 
                "name": proc_data["name"],
                "user": user_map.get(pid, "unknown"), 
                "mem": proc_data["mem"],
                "time": proc_data["time"] # ADDED
            })
            
    server_data["gpus"] = list(gpus.values())
    return server_data

# --- Main execution ---
# This just returns the data, doesn't put it in the "all_stats" wrapper
final_data = fetch_server_stats(SERVER_NAME)

with open(OUTPUT_FILE, 'w') as f:
    json.dump(final_data, f, indent=2)

print(f"Successfully wrote local stats to {OUTPUT_FILE}")

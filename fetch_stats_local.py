import subprocess
import json
import os
import socket

# --- Configuration ---
SERVER_NAME = socket.gethostname().split('.')[0]
OUTPUT_FILE = "my_stats.json"
# ---------------------

def safe_int(value, default=0):
    """Converts a value to int, returning default on failure."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_float(value, default=0.0):
    """Converts a value to float, returning default on failure."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

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
    server_data = {"name": server_name, "gpus": [], "cpu_util": 0, "error": None}
    
    # 1. Get CPU stats
    cpu_command = "top -bn1 | grep '%Cpu(s)' | awk '{print $2 + $4}'"
    cpu_output = run_local_command(cpu_command)
    server_data["cpu_util"] = safe_float(cpu_output.strip()) if cpu_output else 0

    # 2. Get GPU stats (Added 'name')
    gpu_query = "nvidia-smi --query-gpu=index,memory.used,memory.total,utilization.gpu,uuid,name --format=csv,noheader,nounits"
    gpu_output = run_local_command(gpu_query)
    
    if gpu_output is None:
        server_data["error"] = "Failed to run nvidia-smi."
        return server_data

    gpus = {}
    for line in gpu_output.splitlines():
        parts = [p.strip() for p in line.split(',')]
        if len(parts) < 6: continue
        gpus[parts[4]] = { # uuid is parts[4]
            "index": safe_int(parts[0]), 
            "mem_used": safe_int(parts[1]),
            "mem_total": safe_int(parts[2]), 
            "util": safe_int(parts[3]),
            "name": parts[5],
            "processes": []
        }

    # 3. Get Process stats (FIXED: query-processes and added 'elapsed_time')
    proc_query = "nvidia-smi --query-processes=gpu_uuid,pid,process_name,used_gpu_memory,elapsed_time --format=csv,noheader,nounits"
    proc_output = run_local_command(proc_query)
    
    pid_to_gpu = {}
    pids_on_server = []
    
    if proc_output:
        for line in proc_output.splitlines():
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 5: continue
            gpu_uuid, pid, proc_name = parts[:3]
            mem_used = parts[3] # Can be 'N/A'
            elapsed_time = parts[4]
            
            pids_on_server.append(pid)
            pid_to_gpu[pid] = {
                "uuid": gpu_uuid, 
                "name": proc_name, 
                "mem": safe_int(mem_used), 
                "time": elapsed_time
            }

    # 4. Get Usernames
    user_map = get_username_from_pid(pids_on_server)

    # 5. Combine all data
    for pid, proc_data in pid_to_gpu.items():
        gpu_uuid = proc_data["uuid"]
        user = user_map.get(pid, "unknown")
        
        # --- FIX: Filter out root processes ---
        if gpu_uuid in gpus and user != "root":
            gpus[gpu_uuid]["processes"].append({
                "pid": pid, 
                "name": proc_data["name"],
                "user": user, 
                "mem": proc_data["mem"],
                "time": proc_data["time"]
            })
            
    server_data["gpus"] = list(gpus.values())
    return server_data

# --- Main execution ---
final_data = fetch_server_stats(SERVER_NAME)

with open(OUTPUT_FILE, 'w') as f:
    json.dump(final_data, f, indent=2)

print(f"Successfully wrote local stats to {OUTPUT_FILE}")

import subprocess
import json
from datetime import datetime

# --- Configuration ---
SERVERS = {
    "lambda": "monitor@129.10.187.52",
    # Add more servers here, e.g.:
    # "titan": "your_ssh_user@129.10.187.53",
}
OUTPUT_FILE = "status.json"
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
    if not pids:
        return {}
    
    pid_str = ",".join(pids)
    # Use ps to get pid and username, skipping the header
    command = f"ps -o pid=,user= -p {pid_str}"
    output = run_ssh_command(server_address, command)
    
    if not output:
        return {}
        
    users = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 2:
            users[parts[0]] = parts[1] # {pid: user}
    return users

def fetch_server_stats(server_name, server_address):
    """Fetches all GPU and process stats from a single server."""
    print(f"Querying {server_name}...")
    server_data = {"name": server_name, "gpus": [], "error": None}
    
    # 1. Get GPU stats
    # We query for index, memory used, memory total, and utilization
    gpu_query = "nvidia-smi --query-gpu=index,memory.used,memory.total,utilization.gpu,uuid --format=csv,noheader,nounits"
    gpu_output = run_ssh_command(server_address, gpu_query)
    
    if gpu_output is None:
        server_data["error"] = "Failed to connect or run nvidia-smi."
        return server_data

    gpus = {}
    for line in gpu_output.splitlines():
        parts = line.split(', ')
        gpus[parts[4]] = { # Keyed by UUID
            "index": int(parts[0]),
            "mem_used": int(parts[1]),
            "mem_total": int(parts[2]),
            "util": int(parts[3]),
            "processes": []
        }

    # 2. Get Process stats
    # We query for GPU UUID (to match), PID, process name, and memory used
    proc_query = "nvidia-smi --query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory --format=csv,noheader,nounits"
    proc_output = run_ssh_command(server_address, proc_query)
    
    pid_to_gpu = {}
    pids_on_server = []
    
    if proc_output:
        for line in proc_output.splitlines():
            parts = line.split(', ')
            gpu_uuid, pid, proc_name, mem_used = parts
            pids_on_server.append(pid)
            pid_to_gpu[pid] = {
                "uuid": gpu_uuid,
                "name": proc_name,
                "mem": int(mem_used)
            }

    # 3. Get usernames for those PIDs
    user_map = get_username_from_pid(server_address, pids_on_server)

    # 4. Combine all data
    for pid, proc_data in pid_to_gpu.items():
        gpu_uuid = proc_data["uuid"]
        if gpu_uuid in gpus:
            gpus[gpu_uuid]["processes"].append({
                "pid": pid,
                "name": proc_data["name"],
                "user": user_map.get(pid, "unknown"),
                "mem": proc_data["mem"]
            })
            
    server_data["gpus"] = list(gpus.values())
    return server_data

# --- Main execution ---
all_stats = {
    "servers": [],
    "last_updated": datetime.utcnow().isoformat() + "Z"
}

for name, address in SERVERS.items():
    stats = fetch_server_stats(name, address)
    all_stats["servers"].append(stats)

# Write the final JSON file
with open(OUTPUT_FILE, 'w') as f:
    json.dump(all_stats, f, indent=2)

print(f"Successfully wrote stats to {OUTPUT_FILE}")

import subprocess
import json
from datetime import datetime
import os
import glob # New import to scan for files
import time # New import to check file age
import achievements # Achievement system

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
AGGREGATE_FILE = os.path.join(script_dir, 'aggregate_stats.json')
ALIAS_FILE = os.path.join(script_dir, 'user_aliases.json')
ACHIEVEMENTS_FILE = os.path.join(script_dir, 'achievements.json')
INCOMING_DIR = os.path.join(script_dir, 'incoming')
STALE_THRESHOLD_SECONDS = 300 # 5 minutes
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

def run_ssh_command(server_address, command):
    """Runs a command on a remote server via SSH and returns the output."""
    try:
        cmd = ["ssh", server_address, command]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=10)
        return result.stdout.strip()
    except Exception as e:
        print(f"Error connecting to {server_address}: {e}")
        return None


def load_alias_map(path):
    """Loads a map of aliases to canonical usernames."""
    try:
        with open(path) as alias_file:
            raw_map = json.load(alias_file)
            return {key.lower(): value for key, value in raw_map.items()}
    except FileNotFoundError:
        return {}
    except Exception as exc:
        print(f"Warning: failed to read alias map {path}: {exc}")
        return {}


def canonicalize_user(username, alias_map):
    if not username:
        return 'unknown'
    return alias_map.get(username.lower(), username)


def compute_snapshot_totals(all_stats, alias_map):
    totals = {}
    total_capacity = 0

    for server in all_stats.get("servers", []):
        if server.get("error"):
            continue

        server_name = server.get("name", "unknown")

        # Include CPU in hog logic
        if server.get("cpu_util", 0) > 50: # 50% CPU hog threshold
             for gpu in server.get("gpus", []):
                for proc in gpu.get("processes", []):
                    raw_user = proc.get("user") or "unknown"
                    if raw_user == "root": continue # Filter root
                    canonical = canonicalize_user(raw_user, alias_map)
                    if canonical not in totals:
                         totals[canonical] = { "mem": 0, "machines": set(), "raw_users": set() }
                    totals[canonical]["machines"].add(f"{server_name} (CPU)")


        for gpu in server.get("gpus", []):
            total_capacity += gpu.get("mem_total", 0)
            for proc in gpu.get("processes", []):
                raw_user = proc.get("user") or "unknown"
                if raw_user == "root": continue # Filter root
                canonical = canonicalize_user(raw_user, alias_map)

                if canonical not in totals:
                    totals[canonical] = {
                        "mem": 0,
                        "machines": set(),
                        "raw_users": set()
                    }

                entry = totals[canonical]
                entry["mem"] += proc.get("mem", 0)
                entry["machines"].add(server_name)
                entry["raw_users"].add(raw_user)

    return totals, total_capacity


def update_aggregate_file(snapshot_totals, snapshot_capacity):
    now_ts = datetime.utcnow().isoformat() + "Z"

    default_payload = {
        "users": {},
        "cluster": {
            "total_capacity_accum": 0,
            "samples": 0,
            "last_capacity": 0,
            "last_updated": None
        },
        "updated_at": None
    }

    if os.path.exists(AGGREGATE_FILE):
        try:
            with open(AGGREGATE_FILE) as aggregate_file:
                aggregate_data = json.load(aggregate_file)
        except Exception as exc:
            print(f"Warning: failed to parse existing aggregate stats, resetting file: {exc}")
            aggregate_data = default_payload
    else:
        aggregate_data = default_payload

    aggregate_data.setdefault("users", {})
    aggregate_data.setdefault("cluster", {})

    cluster_info = aggregate_data["cluster"]
    cluster_info["total_capacity_accum"] = cluster_info.get("total_capacity_accum", 0) + snapshot_capacity
    cluster_info["samples"] = cluster_info.get("samples", 0) + 1
    cluster_info["last_capacity"] = snapshot_capacity
    cluster_info["last_updated"] = now_ts

    for user, info in snapshot_totals.items():
        entry = aggregate_data["users"].get(user, {
            "total_mem_accum": 0,
            "total_gb_hours": 0.0, # NEW
            "samples": 0,
            "avg_mem": 0,
            "last_sample_mem": 0,
            "last_seen": None,
            "first_seen": now_ts,
            "all_machines": [],
            "last_sample_machines": [],
            "raw_users_seen": []
        })

        entry["total_mem_accum"] += info["mem"] # This is MiB-minutes (assuming 1-min cron)
        entry["samples"] += 1
        entry["last_sample_mem"] = info["mem"]
        entry["last_seen"] = now_ts
        entry.setdefault("first_seen", now_ts)
        
        # --- FIX: Calculate GB-Hours correctly ---
        # (total_mem_accum is MiB-minutes / 1024 MiB/GB) / 60 min/hr
        entry["total_gb_hours"] = entry["total_mem_accum"] / 1024.0 / 60.0

        machines_seen = set(entry.get("all_machines", []))
        machines_seen.update(info["machines"])
        entry["all_machines"] = sorted(machines_seen)
        entry["last_sample_machines"] = sorted(info["machines"])

        raw_seen = set(entry.get("raw_users_seen", []))
        raw_seen.update(info["raw_users"])
        entry["raw_users_seen"] = sorted(raw_seen)

        entry["avg_mem"] = entry["total_mem_accum"] / entry["samples"] if entry["samples"] else 0

        aggregate_data["users"][user] = entry

    aggregate_data["updated_at"] = now_ts

    try:
        with open(AGGREGATE_FILE, 'w') as aggregate_file:
            json.dump(aggregate_data, aggregate_file, indent=2)
    except Exception as exc:
        print(f"Warning: failed to write aggregate stats to {AGGREGATE_FILE}: {exc}")

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
    """Fetches all GPU, CPU, and process stats from a single server."""
    print(f"Querying {server_name}...")
    server_data = {"name": server_name, "gpus": [], "cpu_util": 0, "error": None}
    
    # 1. Get CPU stats
    cpu_command = "top -bn1 | grep '%Cpu(s)' | awk '{print $2 + $4}'"
    cpu_output = run_ssh_command(server_address, cpu_command)
    server_data["cpu_util"] = safe_float(cpu_output.strip()) if cpu_output else 0
    
    # 2. Get GPU stats (Added 'name')
    gpu_query = "nvidia-smi --query-gpu=index,memory.used,memory.total,utilization.gpu,uuid,name --format=csv,noheader,nounits"
    gpu_output = run_ssh_command(server_address, gpu_query)
    
    if gpu_output is None:
        server_data["error"] = "Failed to connect or run nvidia-smi."
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
    proc_query = "nvidia-smi --query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory --format=csv,noheader,nounits"
    proc_output = run_ssh_command(server_address, proc_query)
    
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
    user_map = get_username_from_pid(server_address, pids_on_server)

    # 5. Combine data
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
all_stats = {
    "servers": [],
    "last_updated": datetime.utcnow().isoformat() + "Z"
}

alias_map = load_alias_map(ALIAS_FILE)

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
        file_mtime = os.path.getmtime(file_path)
        if (current_time - file_mtime) > STALE_THRESHOLD_SECONDS:
            machine_name = os.path.basename(file_path).split('.')[0]
            print(f"Found stale data for {machine_name}.")
            all_stats["servers"].append({
                "name": machine_name,
                "gpus": [],
                "error": "Data is stale. Machine may be offline."
            })
            continue 
            
        with open(file_path) as f:
            spoke_data = json.load(f)
            print(f"Adding data for {spoke_data.get('name')}")
            all_stats["servers"].append(spoke_data)
            
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

# 3a. Update aggregate statistics before writing snapshot
snapshot_totals, snapshot_capacity = compute_snapshot_totals(all_stats, alias_map)
update_aggregate_file(snapshot_totals, snapshot_capacity)

# 3b. Check and award achievements
print("Checking for new achievements...")
achievements_data = achievements.load_achievements(ACHIEVEMENTS_FILE)

# Load aggregate stats for lifetime achievements
aggregate_stats = {}
if os.path.exists(AGGREGATE_FILE):
    try:
        with open(AGGREGATE_FILE) as f:
            aggregate_stats = json.load(f)
    except Exception as e:
        print(f"Warning: failed to load aggregate stats for achievements: {e}")
        aggregate_stats = {}

achievements_data, new_achievements = achievements.check_achievements(
    all_stats, aggregate_stats, alias_map, achievements_data
)

# Save updated achievements
achievements.save_achievements(ACHIEVEMENTS_FILE, achievements_data)

# Log new achievements
if new_achievements:
    print(f"\n🎉 NEW ACHIEVEMENTS EARNED! 🎉")
    for achv in new_achievements:
        icon = achv["achievement"].get("icon", "🏆")
        name = achv["achievement"].get("name", "Unknown")
        print(f"  {icon} {achv['user']} earned: {name}")
    print()

# 4. Write the final, combined file
with open(OUTPUT_FILE, 'w') as f:
    json.dump(all_stats, f, indent=2)

print(f"Successfully wrote combined stats to {OUTPUT_FILE}")

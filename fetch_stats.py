import subprocess
import json
from datetime import datetime
import os # <-- Import os

# --- Configuration ---
# Get the directory of the script
script_dir = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(script_dir, 'config.json')

# Load servers from config.json
try:
    with open(config_path) as f:
        config = json.load(f)
    SERVERS = config['servers']
except FileNotFoundError:
    print(f"FATAL: config.json not found at {config_path}")
    exit(1)

OUTPUT_FILE = os.path.join(script_dir, 'status.json')
# ---------------------

# ... (the rest of your python script is unchanged) ...
def run_ssh_command(server_address, command):
# ... (all your functions) ...
def fetch_server_stats(server_name, server_address):
# ... (all your functions) ...

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

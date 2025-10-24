#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$DIR"

echo "Running local stats update..."

# 1. Get the machine's name (e.g., "machine-2")
MACHINE_NAME=$(hostname -s)

# 2. Read hub address and path from the private config.json
#    We use a python one-liner since we know python3 is available.
HUB_INFO=$(python3 -c "import json; f = open('config.json'); d = json.load(f); print(f\"{d['hub_address']}:{d['hub_path']}\"); f.close()")

if [ -z "$HUB_INFO" ]; then
    echo "FATAL: Could not read hub info from config.json"
    exit 1
fi

# 3. Run the local fetcher
python3 fetch_local_stats.py

# 4. Securely copy the result to the 'hub' using the loaded info
scp my_stats.json ${HUB_INFO}${MACHINE_NAME}.json

echo "Sent stats to hub."

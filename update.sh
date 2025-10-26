#!/bin/bash

# Get the directory of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$DIR"

echo "Running GPU stat update..."

# Run the python fetcher
# Make sure you use the correct python3/pip3 aliases
python3 fetch_stats.py

# Commit and push the changes
git add status.json aggregate_stats.json

# Check if there are changes to commit
if ! git diff-index --quiet HEAD --; then
  git commit -m "Auto-update GPU stats: $(date)"
  git push origin updates  # Or whatever your gh-pages branch is
  echo "Stats pushed to GitHub."
else
  echo "No changes detected."
fi

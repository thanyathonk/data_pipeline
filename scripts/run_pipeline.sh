#!/usr/bin/env bash
set -euo pipefail

# Change to repo root
cd "$(dirname "$0")/.."

ENV_NAME="pipeline"

if ! command -v conda >/dev/null 2>&1; then
  echo "[ERROR] conda not found in PATH. Install Anaconda or Miniconda." >&2
  exit 1
fi

# Ensure conda functions are available
source "$(conda info --base)/etc/profile.d/conda.sh"

if ! conda env list | grep -qE "^${ENV_NAME}\\s"; then
  echo "[INFO] Creating conda env '${ENV_NAME}' from environment.yml ..."
  conda env create -f environment.yml
fi

conda activate "$ENV_NAME"

echo "[CLEAN] Removing large temporary indexes under data/split ..."
rm -f data/split/adr_sid_index.sqlite data/split/drug_sid_index.sqlite

echo "[CLEAN] Tidying logs/steps ..."
rm -rf data/logs/steps
mkdir -p data/logs/steps

echo "[CLEAN] Removing cache folders ..."
rm -rf data/.cache

echo "[RUN] Starting pipeline ..."
python run_pipeline.py --no-confirm -y --qps 4 --max-workers 8 "$@"
EXITCODE=$?

echo "[DONE] Pipeline finished with exit code $EXITCODE"
exit $EXITCODE

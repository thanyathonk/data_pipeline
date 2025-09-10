#!/usr/bin/env bash
set -euo pipefail

PY=${PYTHON:-python3}
VENV_DIR=.venv

if [ ! -d "$VENV_DIR" ]; then
  $PY -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"
pip install --upgrade pip
pip install -r requirements.txt

echo "OK: virtualenv ready. Activate with: source $VENV_DIR/bin/activate"


#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
if [[ -z "${CONFIG_FILE}" || ! -f "${CONFIG_FILE}" ]]; then
  echo "Usage: $0 <path-to-config-env>" >&2
  exit 1
fi

source "${CONFIG_FILE}"

VENV_DIR=${VENV_DIR:-.venv}
if [[ ! -d "${VENV_DIR}" ]]; then
  echo "Venv not found. Run scripts/prefect-deploy.sh first." >&2
  exit 1
fi

exec "${VENV_DIR}/bin/python" "$(dirname "$0")/prefect-flow.py"

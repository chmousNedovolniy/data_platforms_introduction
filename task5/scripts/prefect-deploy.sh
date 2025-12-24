#!/usr/bin/env bash
set -euo pipefail

VENV_DIR=${VENV_DIR:-.venv}
PYTHON_BIN=${PYTHON_BIN:-}
SUDO_PASS=${SUDO_PASS:-}

pick_python() {
  if [[ -n "${PYTHON_BIN}" ]]; then
    echo "${PYTHON_BIN}"
    return
  fi
  if command -v python3.12 >/dev/null 2>&1; then
    echo "python3.12"
    return
  fi
  echo "python3"
}

ensure_venv_pkg() {
  if command -v apt-get >/dev/null 2>&1; then
    if [[ -n "${SUDO_PASS}" ]]; then
      printf "%s\n" "${SUDO_PASS}" | sudo -S apt-get update -y
      printf "%s\n" "${SUDO_PASS}" | sudo -S apt-get install -y python3-venv
    else
      sudo apt-get update -y
      sudo apt-get install -y python3-venv
    fi
  else
    echo "apt-get not found; install python3-venv manually." >&2
    exit 1
  fi
}

PYTHON_BIN=$(pick_python)

if [[ ! -d "${VENV_DIR}" || ! -x "${VENV_DIR}/bin/python" ]]; then
  rm -rf "${VENV_DIR}"
  if ! "${PYTHON_BIN}" -m venv "${VENV_DIR}" 2>/tmp/venv.err; then
    err=$(cat /tmp/venv.err || true)
    if echo "${err}" | grep -q "ensurepip is not available"; then
      ensure_venv_pkg
      "${PYTHON_BIN}" -m venv "${VENV_DIR}"
    else
      echo "${err}" >&2
      exit 1
    fi
  fi
fi

"${VENV_DIR}/bin/pip" install --upgrade pip
"${VENV_DIR}/bin/pip" install "prefect==2.16.6" "griffe<1.0.0" "pydantic==2.7.4"

echo "Prefect installed in ${VENV_DIR}"

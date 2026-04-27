#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${PROJECT_DIR}/logs"
LOCK_FILE="${LOG_DIR}/tiendanube_sync.lock"
LOG_FILE="${LOG_DIR}/tiendanube_sync_$(date +%Y%m%d).log"
PYTHON_BIN="${PROJECT_DIR}/.venv/bin/python"

mkdir -p "${LOG_DIR}"

log() {
  printf '[%s] %s\n' "$(date --iso-8601=seconds)" "$*"
}

{
  if ! flock -n 200; then
    log "Otra sincronizacion sigue en ejecucion. Se omite esta corrida."
    exit 0
  fi

  started_at="$(date +%s)"
  log "Inicio sync Tienda Nube."

  if [[ ! -x "${PYTHON_BIN}" ]]; then
    log "ERROR: no existe ${PYTHON_BIN}. Preparar la venv antes de ejecutar."
    exit 1
  fi

  cd "${PROJECT_DIR}"

  set +e
  "${PYTHON_BIN}" -m gn_stock_export sync-tiendanube --config config.toml --env-file .env
  status=$?
  set -e

  finished_at="$(date +%s)"
  duration_seconds=$((finished_at - started_at))

  if [[ ${status} -eq 0 ]]; then
    log "Fin sync Tienda Nube OK en ${duration_seconds}s."
  else
    log "Fin sync Tienda Nube con ERROR ${status} en ${duration_seconds}s."
  fi

  exit "${status}"
} 200>"${LOCK_FILE}" >>"${LOG_FILE}" 2>&1

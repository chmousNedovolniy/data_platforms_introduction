#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
ACTION=${2:-}
if [[ -z "${CONFIG_FILE}" || -z "${ACTION}" ]]; then
  echo "Usage: $0 <path-to-config-env> <start|stop|status>" >&2
  exit 1
fi

source "${CONFIG_FILE}"

: "${HIVESERVER2_HOST:?}"
: "${METASTORE_HOST:?}"
: "${DERBY_HOST:?}"
: "${SSH_USER:?}"
: "${SSH_KEY:?}"
: "${HIVE_HOME:?}"
: "${HIVE_USER:?}"
: "${HIVE_LOG_DIR:?}"
: "${JAVA_HOME:?}"
: "${HIVESERVER2_PORT:?}"
: "${METASTORE_PORT:?}"
: "${DERBY_PORT:?}"

SSH_OPTS=${SSH_OPTS:-}
SUDO_PASS=${SUDO_PASS:-}

log() {
  echo "[$(date +%H:%M:%S)] $*" >&2
}

remote_exec() {
  local host=$1; shift
  ssh -i "${SSH_KEY}" ${SSH_OPTS} "${SSH_USER}@${host}" "$@"
}

remote_sudo() {
  local host=$1; shift
  if [[ -n "${SUDO_PASS}" ]]; then
    remote_exec "${host}" "printf '%s\n' \"${SUDO_PASS}\" | sudo -S bash -c \"$*\""
  else
    remote_exec "${host}" "sudo bash -c \"$*\""
  fi
}

run_as_hadoop() {
  local host=$1 cmd=$2
  local env="export JAVA_HOME=${JAVA_HOME}; export HADOOP_HOME=/opt/hadoop/current; export HADOOP_CONF_DIR=/opt/hadoop/current/etc/hadoop; export HIVE_HOME=${HIVE_HOME}; export HIVE_CONF_DIR=${HIVE_HOME}/conf; export HIVE_LOG_DIR=${HIVE_LOG_DIR}; export HIVE_AUX_JARS_PATH=/usr/share/java/derbyclient.jar"
  remote_sudo "${host}" "su - ${HIVE_USER} -c '${env}; ${cmd}'"
}

do_start() {
  log "Starting Derby Network Server"
  run_as_hadoop "${DERBY_HOST}" "${JAVA_HOME}/bin/java -cp /usr/share/java/derby.jar:/usr/share/java/derbynet.jar org.apache.derby.drda.NetworkServerControl start -h 0.0.0.0 -p ${DERBY_PORT} >/dev/null 2>&1 &"

  log "Initializing Metastore schema (Derby)"
  run_as_hadoop "${METASTORE_HOST}" "${HIVE_HOME}/bin/schematool -dbType derby -initSchema >/dev/null 2>&1 || true"

  log "Starting Hive Metastore"
  run_as_hadoop "${METASTORE_HOST}" "${HIVE_HOME}/bin/hive --service metastore --hiveconf hive.metastore.uris=thrift://${METASTORE_HOST}:${METASTORE_PORT} --hiveconf hive.metastore.uris.privileged=false --hiveconf hive.metastore.uris.external=thrift://${METASTORE_HOST}:${METASTORE_PORT} >/dev/null 2>&1 &"

  log "Starting HiveServer2"
  run_as_hadoop "${HIVESERVER2_HOST}" "${HIVE_HOME}/bin/hive --service hiveserver2 --hiveconf hive.server2.thrift.port=${HIVESERVER2_PORT} >/dev/null 2>&1 &"
}

do_stop() {
  log "Stopping HiveServer2"
  remote_sudo "${HIVESERVER2_HOST}" "pkill -f 'hiveserver2' || true"

  log "Stopping Hive Metastore"
  remote_sudo "${METASTORE_HOST}" "pkill -f 'metastore' || true"

  log "Stopping Derby Network Server"
  run_as_hadoop "${DERBY_HOST}" "${JAVA_HOME}/bin/java -cp /usr/share/java/derby.jar:/usr/share/java/derbynet.jar org.apache.derby.drda.NetworkServerControl shutdown -h 0.0.0.0 -p ${DERBY_PORT} >/dev/null 2>&1 || true"
}

do_status() {
  for host in "${DERBY_HOST}" "${METASTORE_HOST}" "${HIVESERVER2_HOST}"; do
    log "jps on ${host}"
    remote_sudo "${host}" "su - ${HIVE_USER} -c 'jps -l' || true"
  done
  log "Ports check (local from each host)"
  remote_exec "${DERBY_HOST}" "ss -ltn | grep -E ':${DERBY_PORT} ' || true"
  remote_exec "${METASTORE_HOST}" "ss -ltn | grep -E ':${METASTORE_PORT} ' || true"
  remote_exec "${HIVESERVER2_HOST}" "ss -ltn | grep -E ':${HIVESERVER2_PORT} ' || true"
}

case "${ACTION}" in
  start) do_start ;;
  stop) do_stop ;;
  status) do_status ;;
  *)
    echo "Unknown action: ${ACTION}" >&2
    exit 1
    ;;
esac

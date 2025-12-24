#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
ACTION=${2:-}
if [[ -z "${CONFIG_FILE}" || -z "${ACTION}" ]]; then
  echo "Usage: $0 <path-to-config-env> <start|stop|status>" >&2
  exit 1
fi

source "${CONFIG_FILE}"

: "${RESOURCEMANAGER_HOST:?}"
: "${HISTORYSERVER_HOST:?}"
: "${NODEMANAGER_HOSTS:?}"
: "${SSH_USER:?}"
: "${SSH_KEY:?}"
: "${HADOOP_PREFIX:?}"
: "${HADOOP_HOME:?}"
: "${HADOOP_USER:?}"
: "${JAVA_HOME:?}"
: "${YARN_SYSLOG_DIR:?}"
: "${HIST_WEB_PORT:?}"
: "${HIST_RPC_PORT:?}"

SSH_OPTS=${SSH_OPTS:-}
SUDO_PASS=${SUDO_PASS:-}
ALL_HOSTS=("${RESOURCEMANAGER_HOST}" "${NODEMANAGER_HOSTS[@]}")

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

yarn_daemon() {
  local host=$1 role=$2 action=$3
  local cmd="export JAVA_HOME=${JAVA_HOME}; export HADOOP_HOME=${HADOOP_HOME}; export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop; export HADOOP_LOG_DIR=${YARN_SYSLOG_DIR}; ${HADOOP_HOME}/bin/yarn --daemon ${action} ${role}"
  remote_sudo "${host}" "su - ${HADOOP_USER} -c '${cmd}'"
}

mr_daemon() {
  local host=$1 action=$2
  local cmd="export JAVA_HOME=${JAVA_HOME}; export HADOOP_HOME=${HADOOP_HOME}; export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop; export HADOOP_LOG_DIR=${YARN_SYSLOG_DIR}; ${HADOOP_HOME}/bin/mapred --daemon ${action} historyserver"
  remote_sudo "${host}" "su - ${HADOOP_USER} -c '${cmd}'"
}

do_start() {
  log "Starting ResourceManager"
  yarn_daemon "${RESOURCEMANAGER_HOST}" "resourcemanager" "start"

  log "Starting JobHistoryServer"
  mr_daemon "${HISTORYSERVER_HOST}" "start"

  for nm in "${NODEMANAGER_HOSTS[@]}"; do
    log "Starting NodeManager on ${nm}"
    yarn_daemon "${nm}" "nodemanager" "start"
  done
}

do_stop() {
  for nm in "${NODEMANAGER_HOSTS[@]}"; do
    log "Stopping NodeManager on ${nm}"
    yarn_daemon "${nm}" "nodemanager" "stop" || true
  done

  log "Stopping JobHistoryServer"
  mr_daemon "${HISTORYSERVER_HOST}" "stop" || true

  log "Stopping ResourceManager"
  yarn_daemon "${RESOURCEMANAGER_HOST}" "resourcemanager" "stop" || true
}

do_status() {
  for host in "${ALL_HOSTS[@]}"; do
    log "jps on ${host}"
    remote_sudo "${host}" "su - ${HADOOP_USER} -c 'jps -l' || true"
  done
  if [[ ! " ${NODEMANAGER_HOSTS[*]} " =~ " ${HISTORYSERVER_HOST} " ]]; then
    log "jps on ${HISTORYSERVER_HOST}"
    remote_sudo "${HISTORYSERVER_HOST}" "su - ${HADOOP_USER} -c 'jps -l' || true"
  fi
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

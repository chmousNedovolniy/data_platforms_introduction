#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
ACTION=${2:-}
if [[ -z "${CONFIG_FILE}" || -z "${ACTION}" ]]; then
  echo "Usage: $0 <path-to-config-env> <format|start|stop|status>" >&2
  exit 1
fi

source "${CONFIG_FILE}"

: "${NAMENODE_HOST:?}"
: "${SECONDARY_NN_HOST:?}"
: "${DATANODE_HOSTS:?}"
: "${SSH_USER:?}"
: "${SSH_KEY:?}"
: "${HADOOP_PREFIX:?}"
: "${HADOOP_USER:?}"
: "${HADOOP_LOG_DIR:?}"
: "${JAVA_HOME:?}"

SSH_OPTS=${SSH_OPTS:-}
SUDO_PASS=${SUDO_PASS:-}
HADOOP_HOME="${HADOOP_PREFIX}/current"
HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
ALL_HOSTS=("${NAMENODE_HOST}" "${SECONDARY_NN_HOST}" "${DATANODE_HOSTS[@]}")

log() {
  echo "[$(date +%H:%M:%S)] $*"
}

if [[ ${#DATANODE_HOSTS[@]} -lt 3 ]]; then
  log "Warning: expected 3 DataNodes, found ${#DATANODE_HOSTS[@]}"
fi

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

hadoop_daemon() {
  local host=$1 role=$2 action=$3
  local cmd="export JAVA_HOME=${JAVA_HOME}; export HADOOP_HOME=${HADOOP_HOME}; export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}; export HADOOP_LOG_DIR=${HADOOP_LOG_DIR}; ${HADOOP_HOME}/bin/hdfs --daemon ${action} ${role}"
  remote_sudo "${host}" "su - ${HADOOP_USER} -c '${cmd}'"
}

do_format() {
  log "Formatting NameNode on ${NAMENODE_HOST}"
  if remote_sudo "${NAMENODE_HOST}" "test -f ${HDFS_NAME_DIR}/current/VERSION"; then
    log "NameNode already formatted; skipping."
    return
  fi
  remote_sudo "${NAMENODE_HOST}" "su - ${HADOOP_USER} -c 'export JAVA_HOME=${JAVA_HOME}; export HADOOP_HOME=${HADOOP_HOME}; export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}; ${HADOOP_HOME}/bin/hdfs namenode -format -force -nonInteractive'"
  log "Format completed."
}

do_start() {
  log "Starting NameNode"
  hadoop_daemon "${NAMENODE_HOST}" "namenode" "start"

  log "Starting Secondary NameNode"
  hadoop_daemon "${SECONDARY_NN_HOST}" "secondarynamenode" "start"

  for dn in "${DATANODE_HOSTS[@]}"; do
    log "Starting DataNode on ${dn}"
    hadoop_daemon "${dn}" "datanode" "start"
  done
}

do_stop() {
  for dn in "${DATANODE_HOSTS[@]}"; do
    log "Stopping DataNode on ${dn}"
    hadoop_daemon "${dn}" "datanode" "stop" || true
  done

  log "Stopping Secondary NameNode"
  hadoop_daemon "${SECONDARY_NN_HOST}" "secondarynamenode" "stop" || true

  log "Stopping NameNode"
  hadoop_daemon "${NAMENODE_HOST}" "namenode" "stop" || true
}

do_status() {
  for host in "${ALL_HOSTS[@]}"; do
    log "jps on ${host}"
    remote_sudo "${host}" "su - ${HADOOP_USER} -c 'jps -l' || true"
  done
}

case "${ACTION}" in
  format) do_format ;;
  start) do_start ;;
  stop) do_stop ;;
  status) do_status ;;
  *)
    echo "Unknown action: ${ACTION}" >&2
    exit 1
    ;;
esac

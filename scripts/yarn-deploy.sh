#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
if [[ -z "${CONFIG_FILE}" || ! -f "${CONFIG_FILE}" ]]; then
  echo "Usage: $0 <path-to-config-env>" >&2
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
: "${YARN_LOCAL_DIR:?}"
: "${YARN_LOG_DIR:?}"
: "${YARN_SYSLOG_DIR:?}"
: "${JAVA_HOME:?}"
: "${RM_ADDR_PORT:?}"
: "${RM_SCHED_PORT:?}"
: "${RM_TRACK_PORT:?}"
: "${RM_WEB_PORT:?}"
: "${NM_WEB_PORT:?}"
: "${HIST_RPC_PORT:?}"
: "${HIST_WEB_PORT:?}"

SSH_OPTS=${SSH_OPTS:-}
SUDO_PASS=${SUDO_PASS:-}
ALL_HOSTS=("${RESOURCEMANAGER_HOST}" "${HISTORYSERVER_HOST}" "${NODEMANAGER_HOSTS[@]}")

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

write_remote_file() {
  local host=$1 path=$2 content=$3
  if [[ -n "${SUDO_PASS}" ]]; then
    printf "%s\n%s" "${SUDO_PASS}" "${content}" | remote_exec "${host}" "sudo -S tee ${path} >/dev/null"
  else
    printf "%s\n" "${content}" | remote_exec "${host}" "sudo tee ${path} >/dev/null"
  fi
}

ensure_dirs() {
  local host=$1
  log "Preparing dirs on ${host}"
  remote_sudo "${host}" "
    mkdir -p ${YARN_LOCAL_DIR} ${YARN_LOG_DIR} ${YARN_SYSLOG_DIR};
    chown -R ${HADOOP_USER}:${HADOOP_USER} ${YARN_LOCAL_DIR} ${YARN_LOG_DIR};
    chown -R ${HADOOP_USER}:${HADOOP_USER} ${YARN_SYSLOG_DIR};
  "
}

push_configs() {
  local host=$1
  log "Pushing YARN configs to ${host}"
  remote_sudo "${host}" "mkdir -p ${HADOOP_HOME}/etc/hadoop"
  local yarn_site mapred_site yarn_env workers

  yarn_site=$(cat <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${RESOURCEMANAGER_HOST}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>${RESOURCEMANAGER_HOST}:${RM_ADDR_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>${RESOURCEMANAGER_HOST}:${RM_SCHED_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>${RESOURCEMANAGER_HOST}:${RM_TRACK_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>0.0.0.0:${RM_WEB_PORT}</value>
  </property>
  <property>
    <name>yarn.nodemanager.webapp.address</name>
    <value>0.0.0.0:${NM_WEB_PORT}</value>
  </property>
  <property>
    <name>yarn.nodemanager.local-dirs</name>
    <value>file://${YARN_LOCAL_DIR}</value>
  </property>
  <property>
    <name>yarn.nodemanager.log-dirs</name>
    <value>file://${YARN_LOG_DIR}</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.log-aggregation-enable</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.log.server.url</name>
    <value>http://${HISTORYSERVER_HOST}:${HIST_WEB_PORT}/jobhistory/logs/</value>
  </property>
  <property>
    <name>yarn.log-aggregation.retain-seconds</name>
    <value>604800</value>
  </property>
</configuration>
EOF
)

  mapred_site=$(cat <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>${HISTORYSERVER_HOST}:${HIST_RPC_PORT}</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>${HISTORYSERVER_HOST}:${HIST_WEB_PORT}</value>
  </property>
</configuration>
EOF
)

  yarn_env=$(cat <<EOF
export JAVA_HOME=${JAVA_HOME}
export HADOOP_YARN_USER=${HADOOP_USER}
export YARN_RESOURCEMANAGER_USER=${HADOOP_USER}
export YARN_NODEMANAGER_USER=${HADOOP_USER}
export YARN_TIMELINESERVER_USER=${HADOOP_USER}
export HADOOP_LOG_DIR=${YARN_SYSLOG_DIR}
EOF
)

  workers=$(printf "%s\n" "${NODEMANAGER_HOSTS[@]}")

  write_remote_file "${host}" "${HADOOP_HOME}/etc/hadoop/yarn-site.xml" "${yarn_site}"
  write_remote_file "${host}" "${HADOOP_HOME}/etc/hadoop/mapred-site.xml" "${mapred_site}"
  write_remote_file "${host}" "${HADOOP_HOME}/etc/hadoop/yarn-env.sh" "${yarn_env}"
  write_remote_file "${host}" "${HADOOP_HOME}/etc/hadoop/workers" "${workers}"
  remote_sudo "${host}" "chown -R ${HADOOP_USER}:${HADOOP_USER} ${HADOOP_HOME}/etc/hadoop"
}

main() {
  for host in "${ALL_HOSTS[@]}"; do
    ensure_dirs "${host}"
    push_configs "${host}"
    log "Finished ${host}"
  done
  log "YARN deployment completed for all hosts."
}

main "$@"

#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
if [[ -z "${CONFIG_FILE}" || ! -f "${CONFIG_FILE}" ]]; then
  echo "Usage: $0 <path-to-config-env>" >&2
  exit 1
fi

source "${CONFIG_FILE}"

: "${HIVESERVER2_HOST:?}"
: "${HIVESERVER2_PORT:?}"
: "${SSH_USER:?}"
: "${SSH_KEY:?}"
: "${HIVE_USER:?}"
: "${HIVE_HOME:?}"
: "${HIVE_LOG_DIR:?}"
: "${HADOOP_HOME:?}"
: "${HDFS_NAMENODE_URI:?}"
: "${JAVA_HOME:?}"

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

main() {
  log "Preparing sample data on ${HIVESERVER2_HOST}"
  remote_sudo "${HIVESERVER2_HOST}" "
    mkdir -p /tmp/hive-load
    cat >/tmp/hive-load/events_2025-01-01.csv <<'EOF'
1,click
2,view
3,click
EOF
    cat >/tmp/hive-load/events_2025-01-02.csv <<'EOF'
4,view
5,click
EOF
    cat >/tmp/hive-load/load.sql <<'EOSQL'
CREATE DATABASE IF NOT EXISTS demo;
USE demo;
CREATE TABLE IF NOT EXISTS events (
  id INT,
  event STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
LOAD DATA INPATH '/tmp/hive-load/events_2025-01-01.csv' INTO TABLE events PARTITION (dt='2025-01-01');
LOAD DATA INPATH '/tmp/hive-load/events_2025-01-02.csv' INTO TABLE events PARTITION (dt='2025-01-02');
SHOW PARTITIONS events;
SELECT COUNT(*) AS total_rows FROM events;
EOSQL
    chown -R ${HIVE_USER}:${HIVE_USER} /tmp/hive-load
  "

  log "Uploading data to HDFS"
  remote_sudo "${HIVESERVER2_HOST}" "su - ${HIVE_USER} -c '
    export JAVA_HOME=${JAVA_HOME}
    export HADOOP_HOME=${HADOOP_HOME}
    export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
    ${HADOOP_HOME}/bin/hdfs dfs -fs ${HDFS_NAMENODE_URI} -mkdir -p /tmp/hive-load
    ${HADOOP_HOME}/bin/hdfs dfs -fs ${HDFS_NAMENODE_URI} -put -f /tmp/hive-load/events_2025-01-01.csv /tmp/hive-load/
    ${HADOOP_HOME}/bin/hdfs dfs -fs ${HDFS_NAMENODE_URI} -put -f /tmp/hive-load/events_2025-01-02.csv /tmp/hive-load/
  '"

  log "Loading data via beeline"
  remote_sudo "${HIVESERVER2_HOST}" "su - ${HIVE_USER} -c '
    export HIVE_LOG_DIR=${HIVE_LOG_DIR}
    ${HIVE_HOME}/bin/beeline -u jdbc:hive2://${HIVESERVER2_HOST}:${HIVESERVER2_PORT} -f /tmp/hive-load/load.sql
  '"
  log "Load completed."
}

main "$@"

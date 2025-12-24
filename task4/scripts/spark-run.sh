#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
if [[ -z "${CONFIG_FILE}" || ! -f "${CONFIG_FILE}" ]]; then
  echo "Usage: $0 <path-to-config-env>" >&2
  exit 1
fi

source "${CONFIG_FILE}"

: "${SPARK_CLIENT_HOST:?}"
: "${SSH_USER:?}"
: "${SSH_KEY:?}"
: "${SPARK_HOME:?}"
: "${SPARK_USER:?}"
: "${HADOOP_HOME:?}"
: "${HDFS_NAMENODE_URI:?}"
: "${HDFS_INPUT_PATH:?}"
: "${HDFS_OUTPUT_TABLE:?}"
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

copy_job() {
  local host=$1
  local remote_path="/tmp/task4-spark-job.py"
  scp -i "${SSH_KEY}" ${SSH_OPTS} "$(dirname "$0")/spark-job.py" "${SSH_USER}@${host}:${remote_path}"
  echo "${remote_path}"
}

main() {
  local remote_job
  log "Copying job to ${SPARK_CLIENT_HOST}"
  remote_job=$(copy_job "${SPARK_CLIENT_HOST}")

  log "Submitting Spark job on YARN"
  remote_sudo "${SPARK_CLIENT_HOST}" "su - ${SPARK_USER} -c '
    export JAVA_HOME=${JAVA_HOME}
    export HADOOP_HOME=${HADOOP_HOME}
    export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
    export YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop
    export SPARK_HOME=${SPARK_HOME}
    export TASK4_INPUT_PATH=${HDFS_INPUT_PATH}
    export TASK4_OUTPUT_TABLE=${HDFS_OUTPUT_TABLE}
    ${SPARK_HOME}/bin/spark-submit \
      --master yarn \
      --deploy-mode client \
      --conf spark.yarn.appMasterEnv.JAVA_HOME=${JAVA_HOME} \
      --conf spark.executorEnv.JAVA_HOME=${JAVA_HOME} \
      --conf spark.hadoop.fs.defaultFS=${HDFS_NAMENODE_URI} \
      ${remote_job}
  '"

  log "Spark job completed."
}

main "$@"

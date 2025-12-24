#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
if [[ -z "${CONFIG_FILE}" || ! -f "${CONFIG_FILE}" ]]; then
  echo "Usage: $0 <path-to-config-env>" >&2
  exit 1
fi

source "${CONFIG_FILE}"

: "${SPARK_CLIENT_HOST:?}"
: "${HIVESERVER2_HOST:?}"
: "${METASTORE_HOST:?}"
: "${SSH_USER:?}"
: "${SSH_KEY:?}"
: "${SPARK_VERSION:?}"
: "${SPARK_TGZ_URL:?}"
: "${SPARK_PREFIX:?}"
: "${SPARK_HOME:?}"
: "${SPARK_USER:?}"
: "${SPARK_LOG_DIR:?}"
: "${HADOOP_HOME:?}"
: "${HDFS_NAMENODE_URI:?}"
: "${METASTORE_PORT:?}"
: "${JAVA_HOME:?}"

SSH_OPTS=${SSH_OPTS:-}
SUDO_PASS=${SUDO_PASS:-}
LOCAL_TGZ=${LOCAL_TGZ:-/tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz}

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

ensure_local_tgz() {
  local urls=(
    "${SPARK_TGZ_URL}"
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  )
  for url in "${urls[@]}"; do
    if [[ ! -f "${LOCAL_TGZ}" ]]; then
      log "Downloading Spark ${SPARK_VERSION} to ${LOCAL_TGZ} from ${url}"
      curl -L --fail -o "${LOCAL_TGZ}" "${url}" || true
    else
      log "Using cached archive ${LOCAL_TGZ}"
    fi
    if tar -tzf "${LOCAL_TGZ}" >/dev/null 2>&1; then
      return
    fi
    rm -f "${LOCAL_TGZ}"
  done
  echo "Failed to download valid Spark archive. Check SPARK_TGZ_URL." >&2
  exit 1
}

copy_tgz_to_host() {
  local host=$1
  local remote_tgz="/tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  log "Copying Spark archive to ${host}:${remote_tgz}"
  scp -i "${SSH_KEY}" ${SSH_OPTS} "${LOCAL_TGZ}" "${SSH_USER}@${host}:${remote_tgz}"
  echo "${remote_tgz}"
}

ensure_packages() {
  local host=$1
  log "Installing packages on ${host}"
  remote_sudo "${host}" "DEBIAN_FRONTEND=noninteractive apt-get update -y && DEBIAN_FRONTEND=noninteractive apt-get install -y curl tar"
}

deploy_spark() {
  local host=$1
  local remote_tgz
  remote_tgz=$(copy_tgz_to_host "${host}")
  log "Deploying Spark to ${host}"
  remote_sudo "${host}" "
    set -e
    mkdir -p ${SPARK_PREFIX} ${SPARK_LOG_DIR}
    tar -xf ${remote_tgz} -C ${SPARK_PREFIX}
    ln -sfn ${SPARK_PREFIX}/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}
    chown -R ${SPARK_USER}:${SPARK_USER} ${SPARK_PREFIX} ${SPARK_LOG_DIR}
  "
}

push_configs() {
  local host=$1
  log "Pushing Spark configs to ${host}"

  local spark_env spark_defaults hive_site

  spark_env=$(cat <<EOT
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export YARN_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export SPARK_LOG_DIR=${SPARK_LOG_DIR}
EOT
)

  spark_defaults=$(cat <<EOT
spark.master yarn
spark.submit.deployMode client
spark.sql.warehouse.dir ${HDFS_NAMENODE_URI}/user/hive/warehouse
spark.sql.catalogImplementation hive
EOT
)

  hive_site=$(cat <<EOT
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://${METASTORE_HOST}:${METASTORE_PORT}</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
</configuration>
EOT
)

  write_remote_file "${host}" "${SPARK_HOME}/conf/spark-env.sh" "${spark_env}"
  write_remote_file "${host}" "${SPARK_HOME}/conf/spark-defaults.conf" "${spark_defaults}"
  write_remote_file "${host}" "${SPARK_HOME}/conf/hive-site.xml" "${hive_site}"
  remote_sudo "${host}" "chown -R ${SPARK_USER}:${SPARK_USER} ${SPARK_HOME}/conf"
}

main() {
  ensure_local_tgz
  ensure_packages "${SPARK_CLIENT_HOST}"
  deploy_spark "${SPARK_CLIENT_HOST}"
  push_configs "${SPARK_CLIENT_HOST}"
  log "Spark deployment completed."
}

main "$@"

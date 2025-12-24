#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
if [[ -z "${CONFIG_FILE}" || ! -f "${CONFIG_FILE}" ]]; then
  echo "Usage: $0 <path-to-config-env>" >&2
  exit 1
fi

source "${CONFIG_FILE}"

: "${HIVESERVER2_HOST:?}"
: "${METASTORE_HOST:?}"
: "${DERBY_HOST:?}"
: "${SSH_USER:?}"
: "${SSH_KEY:?}"
: "${HIVE_VERSION:?}"
: "${HIVE_TGZ_URL:?}"
: "${HIVE_PREFIX:?}"
: "${HIVE_HOME:?}"
: "${HIVE_USER:?}"
: "${HIVE_LOG_DIR:?}"
: "${HIVE_METASTORE_DB:?}"
: "${HADOOP_HOME:?}"
: "${HDFS_NAMENODE_URI:?}"
: "${HDFS_WAREHOUSE:?}"
: "${HDFS_TMP:?}"
: "${HIVESERVER2_PORT:?}"
: "${METASTORE_PORT:?}"
: "${DERBY_PORT:?}"
: "${JAVA_HOME:?}"

SSH_OPTS=${SSH_OPTS:-}
SUDO_PASS=${SUDO_PASS:-}
LOCAL_TGZ=${LOCAL_TGZ:-/tmp/apache-hive-${HIVE_VERSION}-bin.tar.gz}
declare -A SEEN_HOSTS
ALL_HOSTS=()
for host in "${HIVESERVER2_HOST}" "${METASTORE_HOST}" "${DERBY_HOST}"; do
  if [[ -z "${SEEN_HOSTS[$host]:-}" ]]; then
    SEEN_HOSTS[$host]=1
    ALL_HOSTS+=("${host}")
  fi
done

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
  local urls=("${HIVE_TGZ_URL}" "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz")
  for url in "${urls[@]}"; do
    if [[ ! -f "${LOCAL_TGZ}" ]]; then
      log "Downloading Hive ${HIVE_VERSION} to ${LOCAL_TGZ} from ${url}"
      curl -L --fail --retry 3 --retry-delay 5 --continue-at - -o "${LOCAL_TGZ}" "${url}" || true
    else
      log "Using cached archive ${LOCAL_TGZ}"
    fi
    if tar -tzf "${LOCAL_TGZ}" >/dev/null 2>&1; then
      return
    fi
    rm -f "${LOCAL_TGZ}"
  done
  echo "Failed to download valid Hive archive. Check HIVE_TGZ_URL." >&2
  exit 1
}

copy_tgz_to_host() {
  local host=$1
  local remote_tgz="/tmp/apache-hive-${HIVE_VERSION}-bin.tar.gz"
  log "Copying Hive archive to ${host}:${remote_tgz}"
  scp -i "${SSH_KEY}" ${SSH_OPTS} "${LOCAL_TGZ}" "${SSH_USER}@${host}:${remote_tgz}"
  echo "${remote_tgz}"
}

ensure_packages() {
  local host=$1
  log "Installing packages on ${host}"
  remote_sudo "${host}" "DEBIAN_FRONTEND=noninteractive apt-get update -y && DEBIAN_FRONTEND=noninteractive apt-get install -y curl tar"
}

deploy_hive_bits() {
  local host=$1
  local remote_tgz
  remote_tgz=$(copy_tgz_to_host "${host}")
  log "Deploying Hive to ${host}"
  remote_sudo "${host}" "
    set -e
    mkdir -p ${HIVE_PREFIX} ${HIVE_LOG_DIR} ${HIVE_METASTORE_DB}
    tar -xf ${remote_tgz} -C ${HIVE_PREFIX}
    ln -sfn ${HIVE_PREFIX}/apache-hive-${HIVE_VERSION}-bin ${HIVE_HOME}
    chown -R ${HIVE_USER}:${HIVE_USER} ${HIVE_PREFIX} ${HIVE_LOG_DIR} ${HIVE_METASTORE_DB}
  "
}

push_configs() {
  local host=$1
  log "Pushing Hive configs to ${host}"
  local hive_site hive_env

  hive_site=$(cat <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>${HDFS_NAMENODE_URI}</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://${METASTORE_HOST}:${METASTORE_PORT}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:derby://${DERBY_HOST}:${DERBY_PORT}/metastore_db;create=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.apache.derby.jdbc.ClientDriver</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>${HDFS_WAREHOUSE}</value>
  </property>
  <property>
    <name>hive.server2.thrift.port</name>
    <value>${HIVESERVER2_PORT}</value>
  </property>
  <property>
    <name>hive.server2.thrift.bind.host</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.external.dir</name>
    <value>${HDFS_WAREHOUSE}</value>
  </property>
</configuration>
EOF
)

  hive_env=$(cat <<EOF
export JAVA_HOME=${JAVA_HOME}
export HADOOP_HOME=/opt/hadoop/current
export HADOOP_CONF_DIR=/opt/hadoop/current/etc/hadoop
export HIVE_HOME=${HIVE_HOME}
export HIVE_CONF_DIR=${HIVE_HOME}/conf
export HIVE_LOG_DIR=${HIVE_LOG_DIR}
export PATH=\$PATH:${HIVE_HOME}/bin
EOF
)

  write_remote_file "${host}" "${HIVE_HOME}/conf/hive-site.xml" "${hive_site}"
  write_remote_file "${host}" "${HIVE_HOME}/conf/hive-env.sh" "${hive_env}"
  remote_sudo "${host}" "chown -R ${HIVE_USER}:${HIVE_USER} ${HIVE_HOME}/conf"
}

prepare_hdfs() {
  log "Preparing HDFS dirs on ${HIVESERVER2_HOST}"
  remote_sudo "${HIVESERVER2_HOST}" "
    su - ${HIVE_USER} -c 'export HADOOP_HOME=${HADOOP_HOME}; export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop;
      ${HADOOP_HOME}/bin/hdfs dfsadmin -fs ${HDFS_NAMENODE_URI} -safemode get | grep -q ON && ${HADOOP_HOME}/bin/hdfs dfsadmin -fs ${HDFS_NAMENODE_URI} -safemode leave || true;
      ${HADOOP_HOME}/bin/hdfs dfs -fs ${HDFS_NAMENODE_URI} -mkdir -p ${HDFS_WAREHOUSE} ${HDFS_TMP};
      ${HADOOP_HOME}/bin/hdfs dfs -fs ${HDFS_NAMENODE_URI} -chmod 1777 ${HDFS_TMP};
      ${HADOOP_HOME}/bin/hdfs dfs -fs ${HDFS_NAMENODE_URI} -chmod 777 ${HDFS_WAREHOUSE};
    ';
  "
}

main() {
  ensure_local_tgz
  for host in "${ALL_HOSTS[@]}"; do
    ensure_packages "${host}"
    deploy_hive_bits "${host}"
    push_configs "${host}"
    log "Finished ${host}"
  done
  prepare_hdfs
  log "Hive deployment completed."
}

main "$@"

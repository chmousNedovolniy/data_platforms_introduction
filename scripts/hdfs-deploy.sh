#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE=${1:-}
if [[ -z "${CONFIG_FILE}" || ! -f "${CONFIG_FILE}" ]]; then
  echo "Usage: $0 <path-to-config-env>" >&2
  exit 1
fi

source "${CONFIG_FILE}"

: "${NAMENODE_HOST:?}"
: "${SECONDARY_NN_HOST:?}"
: "${DATANODE_HOSTS:?}"
: "${SSH_USER:?}"
: "${SSH_KEY:?}"
: "${HADOOP_VERSION:?}"
: "${HADOOP_TGZ_URL:?}"
: "${HADOOP_PREFIX:?}"
: "${HADOOP_USER:?}"
: "${HDFS_NAME_DIR:?}"
: "${HDFS_SECONDARY_DIR:?}"
: "${HDFS_DATA_DIR:?}"
: "${HADOOP_LOG_DIR:?}"
: "${JAVA_HOME:?}"
: "${NAMENODE_RPC_PORT:?}"
: "${NAMENODE_HTTP_PORT:?}"
: "${SECONDARY_HTTP_PORT:?}"
: "${DATANODE_HTTP_PORT:?}"
: "${DATANODE_TRANSFER_PORT:?}"
: "${DATANODE_IPC_PORT:?}"
: "${DFS_REPLICATION:?}"

SSH_OPTS=${SSH_OPTS:-}
SUDO_PASS=${SUDO_PASS:-}
LOCAL_TGZ=${LOCAL_TGZ:-/tmp/hadoop-${HADOOP_VERSION}.tar.gz}

ALL_HOSTS=("${NAMENODE_HOST}" "${SECONDARY_NN_HOST}" "${DATANODE_HOSTS[@]}")
HADOOP_HOME="${HADOOP_PREFIX}/current"
HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"

log() {
  echo "[$(date +%H:%M:%S)] $*" >&2
}

if [[ ${#DATANODE_HOSTS[@]} -lt 3 ]]; then
  log "Warning: expected 3 DataNodes, found ${#DATANODE_HOSTS[@]}"
fi
if [[ ${DFS_REPLICATION} -gt ${#DATANODE_HOSTS[@]} ]]; then
  log "Warning: dfs.replication (${DFS_REPLICATION}) is greater than DataNode count (${#DATANODE_HOSTS[@]})"
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

write_remote_file() {
  local host=$1 path=$2 content=$3
  if [[ -n "${SUDO_PASS}" ]]; then
    # First line is password for sudo -S, remaining lines are file content.
    printf "%s\n%s" "${SUDO_PASS}" "${content}" | remote_exec "${host}" "sudo -S tee ${path} >/dev/null"
  else
    printf "%s\n" "${content}" | remote_exec "${host}" "sudo tee ${path} >/dev/null"
  fi
}

ensure_packages() {
  local host=$1
  log "Installing packages on ${host}"
  remote_sudo "${host}" "DEBIAN_FRONTEND=noninteractive apt-get update -y && DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk-headless rsync curl wget tar ssh"
}

ensure_user_and_dirs() {
  local host=$1
  log "Ensuring user and directories on ${host}"
  remote_sudo "${host}" "
    id -u ${HADOOP_USER} >/dev/null 2>&1 || adduser --disabled-password --gecos '' ${HADOOP_USER};
    mkdir -p ${HADOOP_PREFIX} ${HADOOP_LOG_DIR} ${HDFS_NAME_DIR} ${HDFS_SECONDARY_DIR} ${HDFS_DATA_DIR};
    mkdir -p /tmp/hadoop/mapred/local;
    chown -R ${HADOOP_USER}:${HADOOP_USER} ${HADOOP_PREFIX} ${HADOOP_LOG_DIR} ${HDFS_NAME_DIR} ${HDFS_SECONDARY_DIR} ${HDFS_DATA_DIR};
    chown -R ${HADOOP_USER}:${HADOOP_USER} /tmp/hadoop/mapred;
  "
}

ensure_local_tgz() {
  if [[ ! -f "${LOCAL_TGZ}" ]]; then
    log "Downloading Hadoop ${HADOOP_VERSION} locally to ${LOCAL_TGZ}"
    curl -L -o "${LOCAL_TGZ}" "${HADOOP_TGZ_URL}"
  else
    log "Using cached archive ${LOCAL_TGZ}"
  fi
  if [[ -n "${HADOOP_TGZ_SHA256:-}" ]]; then
    echo "${HADOOP_TGZ_SHA256}  ${LOCAL_TGZ}" | sha256sum -c -
  fi
}

copy_tgz_to_host() {
  local host=$1
  local remote_tgz="/tmp/hadoop-${HADOOP_VERSION}.tar.gz"
  log "Copying Hadoop archive to ${host}:${remote_tgz}"
  scp -i "${SSH_KEY}" ${SSH_OPTS} "${LOCAL_TGZ}" "${SSH_USER}@${host}:${remote_tgz}"
  echo "${remote_tgz}"
}

deploy_hadoop_bits() {
  local host=$1
  log "Deploying Hadoop ${HADOOP_VERSION} to ${host}"
  local remote_tgz
  remote_tgz=$(copy_tgz_to_host "${host}")
  remote_sudo "${host}" "
    set -e
    mkdir -p ${HADOOP_PREFIX}
    tar -xf ${remote_tgz} -C ${HADOOP_PREFIX}
    ln -sfn ${HADOOP_PREFIX}/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}
    chown -R ${HADOOP_USER}:${HADOOP_USER} ${HADOOP_PREFIX}
  "
}

push_profile() {
  local host=$1
  log "Configuring environment profile on ${host}"
  local profile_content
  profile_content=$(cat <<EOF
export HADOOP_HOME=${HADOOP_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
export HADOOP_LOG_DIR=${HADOOP_LOG_DIR}
export JAVA_HOME=${JAVA_HOME}
export PATH=\$PATH:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
EOF
)
  write_remote_file "${host}" "/etc/profile.d/hadoop.sh" "${profile_content}"
}

push_configs() {
  local host=$1
  log "Pushing Hadoop configs to ${host}"
  remote_sudo "${host}" "mkdir -p ${HADOOP_CONF_DIR}"
  local core_site hdfs_site mapred_site hadoop_env workers
  core_site=$(cat <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${NAMENODE_HOST}:${NAMENODE_RPC_PORT}</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/hadoop/tmp</value>
  </property>
</configuration>
EOF
)

  hdfs_site=$(cat <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>${DFS_REPLICATION}</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://${HDFS_NAME_DIR}</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>${NAMENODE_HOST}:${NAMENODE_HTTP_PORT}</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>${NAMENODE_HOST}:${NAMENODE_RPC_PORT}</value>
  </property>
  <property>
    <name>dfs.namenode.secondary.http-address</name>
    <value>${SECONDARY_NN_HOST}:${SECONDARY_HTTP_PORT}</value>
  </property>
  <property>
    <name>dfs.namenode.checkpoint.dir</name>
    <value>file://${HDFS_SECONDARY_DIR}</value>
  </property>
  <property>
    <name>dfs.namenode.checkpoint.edits.dir</name>
    <value>file://${HDFS_SECONDARY_DIR}</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://${HDFS_DATA_DIR}</value>
  </property>
  <property>
    <name>dfs.datanode.address</name>
    <value>0.0.0.0:${DATANODE_TRANSFER_PORT}</value>
  </property>
  <property>
    <name>dfs.datanode.http.address</name>
    <value>0.0.0.0:${DATANODE_HTTP_PORT}</value>
  </property>
  <property>
    <name>dfs.datanode.ipc.address</name>
    <value>0.0.0.0:${DATANODE_IPC_PORT}</value>
  </property>
  <property>
    <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
    <value>false</value>
  </property>
</configuration>
EOF
)

  mapred_site=$(cat <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.cluster.local.dir</name>
    <value>/tmp/hadoop/mapred/local</value>
  </property>
</configuration>
EOF
)

  hadoop_env=$(cat <<EOF
export JAVA_HOME=${JAVA_HOME}
export HADOOP_LOG_DIR=${HADOOP_LOG_DIR}
export HDFS_NAMENODE_USER=${HADOOP_USER}
export HDFS_DATANODE_USER=${HADOOP_USER}
export HDFS_SECONDARYNAMENODE_USER=${HADOOP_USER}
EOF
)

  workers=$(printf "%s\n" "${DATANODE_HOSTS[@]}")

  write_remote_file "${host}" "${HADOOP_CONF_DIR}/core-site.xml" "${core_site}"
  write_remote_file "${host}" "${HADOOP_CONF_DIR}/hdfs-site.xml" "${hdfs_site}"
  write_remote_file "${host}" "${HADOOP_CONF_DIR}/mapred-site.xml" "${mapred_site}"
  write_remote_file "${host}" "${HADOOP_CONF_DIR}/hadoop-env.sh" "${hadoop_env}"
  write_remote_file "${host}" "${HADOOP_CONF_DIR}/workers" "${workers}"
  remote_sudo "${host}" "chown -R ${HADOOP_USER}:${HADOOP_USER} ${HADOOP_CONF_DIR}"
}

main() {
  ensure_local_tgz
  for host in "${ALL_HOSTS[@]}"; do
    ensure_packages "${host}"
    ensure_user_and_dirs "${host}"
    deploy_hadoop_bits "${host}"
    push_profile "${host}"
    push_configs "${host}"
    log "Finished ${host}"
  done
  log "Deployment completed for all hosts."
}

main "$@"

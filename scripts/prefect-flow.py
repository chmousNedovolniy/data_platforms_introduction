#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path

from prefect import flow, task


def _require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise SystemExit(f"Missing required env: {key}")
    return value


@task(name="Check HDFS input")
def check_hdfs_input():
    ssh_user = _require_env("SSH_USER")
    ssh_key = _require_env("SSH_KEY")
    ssh_opts = os.environ.get("SSH_OPTS", "")
    host = _require_env("SPARK_CLIENT_HOST")
    hadoop_home = _require_env("HADOOP_HOME")
    hdfs_nn = _require_env("HDFS_NAMENODE_URI")
    hdfs_input = _require_env("HDFS_INPUT_PATH")
    sudo_pass = os.environ.get("SUDO_PASS", "")

    hdfs_cmd = (
        f"su - hadoop -c '{hadoop_home}/bin/hdfs dfs -fs {hdfs_nn} -ls {hdfs_input}'"
    )
    if sudo_pass:
        remote = f"printf '%s\n' '{sudo_pass}' | sudo -S bash -lc \"{hdfs_cmd}\""
    else:
        remote = f"sudo bash -lc \"{hdfs_cmd}\""

    cmd = [
        "ssh",
        "-i",
        ssh_key,
        *ssh_opts.split(),
        f"{ssh_user}@{host}",
        remote,
    ]
    subprocess.run(cmd, check=True)


@task(name="Run Spark job")
def run_spark_job():
    ssh_user = _require_env("SSH_USER")
    ssh_key = _require_env("SSH_KEY")
    ssh_opts = os.environ.get("SSH_OPTS", "")
    host = _require_env("SPARK_CLIENT_HOST")
    spark_home = _require_env("SPARK_HOME")
    spark_user = _require_env("SPARK_USER")
    hadoop_home = _require_env("HADOOP_HOME")
    hdfs_nn = _require_env("HDFS_NAMENODE_URI")
    hdfs_input = _require_env("HDFS_INPUT_PATH")
    output_table = _require_env("HDFS_OUTPUT_TABLE")
    java_home = _require_env("JAVA_HOME")
    app_name = os.environ.get("SPARK_APP_NAME", "task5-prefect-spark")
    sudo_pass = os.environ.get("SUDO_PASS", "")

    local_job = Path(__file__).resolve().parent / "spark-job.py"
    remote_job = "/tmp/task5-spark-job.py"

    scp_cmd = [
        "scp",
        "-i",
        ssh_key,
        *ssh_opts.split(),
        str(local_job),
        f"{ssh_user}@{host}:{remote_job}",
    ]
    subprocess.run(scp_cmd, check=True)

    spark_cmd = (
        f"export JAVA_HOME={java_home}; "
        f"export HADOOP_HOME={hadoop_home}; "
        f"export HADOOP_CONF_DIR={hadoop_home}/etc/hadoop; "
        f"export YARN_CONF_DIR={hadoop_home}/etc/hadoop; "
        f"export SPARK_HOME={spark_home}; "
        f"export TASK5_INPUT_PATH={hdfs_input}; "
        f"export TASK5_OUTPUT_TABLE={output_table}; "
        f"{spark_home}/bin/spark-submit "
        f"--name {app_name} "
        f"--master yarn "
        f"--deploy-mode client "
        f"--conf spark.yarn.appMasterEnv.JAVA_HOME={java_home} "
        f"--conf spark.executorEnv.JAVA_HOME={java_home} "
        f"--conf spark.hadoop.fs.defaultFS={hdfs_nn} "
        f"{remote_job}"
    )

    if sudo_pass:
        remote = f"printf '%s\n' '{sudo_pass}' | sudo -S bash -lc \"su - {spark_user} -c '{spark_cmd}'\""
    else:
        remote = f"sudo bash -lc \"su - {spark_user} -c '{spark_cmd}'\""

    ssh_cmd = [
        "ssh",
        "-i",
        ssh_key,
        *ssh_opts.split(),
        f"{ssh_user}@{host}",
        remote,
    ]
    subprocess.run(ssh_cmd, check=True)


@flow(name="task5-prefect-spark-flow")
def main_flow():
    check_hdfs_input()
    run_spark_job()


if __name__ == "__main__":
    main_flow()

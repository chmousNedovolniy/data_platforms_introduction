#!/usr/bin/env python3
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, length, when, input_file_name, regexp_extract


def main():
    spark = (
        SparkSession.builder.appName("task4-spark-hive")
        .enableHiveSupport()
        .getOrCreate()
    )

    input_path = os.environ.get("TASK4_INPUT_PATH")
    output_table = os.environ.get("TASK4_OUTPUT_TABLE")
    if not input_path or not output_table:
        raise SystemExit("TASK4_INPUT_PATH or TASK4_OUTPUT_TABLE is not set")

    schema = "id INT, event STRING"
    df = (
        spark.read.option("sep", ",")
        .schema(schema)
        .csv(input_path)
    )

    df = df.withColumn("source_file", input_file_name())
    df = df.withColumn("dt", regexp_extract(col("source_file"), r"dt=([0-9-]+)", 1))

    transformed = (
        df.withColumn("event", trim(col("event")))
        .withColumn("event_upper", upper(col("event")))
        .withColumn("event_len", length(col("event")))
        .withColumn("is_click", when(col("event") == "click", 1).otherwise(0))
        .drop("source_file")
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS demo")

    (
        transformed.write.mode("overwrite")
        .format("parquet")
        .partitionBy("dt")
        .saveAsTable(output_table)
    )

    spark.stop()


if __name__ == "__main__":
    main()

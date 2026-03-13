# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 15:14:46 2026

@author: Walter Roye T. Fanka
"""

from pyspark.sql import SparkSession
import sys


def get_spark_session(app_name: str = "PySparkProductionPipeline") -> SparkSession:
    """
    Create and configure a hardened SparkSession suitable for local development
    and production-style batch pipelines.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)

        # ---- Execution ----
        .master("local[1]")   # more stable than local[*] on Windows
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")

        # ---- Serialization ----
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # ---- SQL & Performance ----
        .config("spark.sql.adaptive.enabled", "true")

        # ---- Fault tolerance ----
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")

        # ---- Python environment ----
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)

        # ---- Timezone safety ----
        .config("spark.sql.session.timeZone", "UTC")

        # ---- Windows stability ----
        .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse")
        .config("spark.hadoop.io.native.lib.available", "false")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
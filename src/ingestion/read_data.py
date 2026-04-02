# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 13:36:18 2026

@author: Walter Roye T. Fanka
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *

def read_transactions(spark: SparkSession, path: str):
    schema = StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("country", StringType(), True),
        StructField("is_fraud", IntegerType(), True),
    ])

    return spark.read.csv(
        path,
        header=True,
        schema=schema
    )

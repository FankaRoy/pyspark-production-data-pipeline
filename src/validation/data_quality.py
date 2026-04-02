# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 13:49:28 2026

@author: Walter Roye T. Fanka
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum


def validate_data(df: DataFrame) -> None:
    critical_columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "batch_id",
        "load_timestamp",
    ]
    # 1. Null checks
    null_checks = df.select([
        spark_sum(col(c).isNull().cast("int")).alias(c)
        for c in critical_columns
    ]).collect()[0].asDict()

    if any(v > 0 for v in null_checks.values()):
        raise ValueError(f"Null values found in critical columns: {null_checks}")
    
    # 2. Negative amount check
    negative_count = df.filter(col("amount") < 0).count()
    if negative_count > 0:
        raise ValueError("Negative transaction amounts detected")
    
    # 3. Valid currency check
    valid_currencies = ["USD", "EUR", "GBP"]
    invalid_currency = df.filter(~col("currency").isin(valid_currencies)).count()
    if invalid_currency > 0:
        raise ValueError("Invalid currency values detected")

    print(f"✅ Data validation passed | rows={df.count()}")

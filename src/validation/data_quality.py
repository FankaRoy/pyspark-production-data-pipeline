# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 13:49:28 2026

@author: Walter Roye T. Fanka
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def validate_data(df: DataFrame) -> None:
    """
    Production-style data quality checks.
    Raises exception if validation fails.
    """

    # 1. Null checks
    null_counts = df.select([
        col(c).isNull().cast("int").alias(c)
        for c in ["transaction_id", "customer_id", "amount"]
    ])

    if null_counts.groupBy().sum().collect()[0][0] > 0:
        raise ValueError("Null values found in critical columns")

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

# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 13:49:28 2026

@author: MEAL ASSISTANT 1
"""

from pyspark.sql.functions import col

def validate_transactions(df):
    checks = {
        "null_amounts": df.filter(col("amount").isNull()).count(),
        "negative_amounts": df.filter(col("amount") < 0).count(),
        "invalid_currency": df.filter(~col("currency").isin("USD", "EUR", "GBP")).count(),
    }

    failed = {k: v for k, v in checks.items() if v > 0}

    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")

    return df
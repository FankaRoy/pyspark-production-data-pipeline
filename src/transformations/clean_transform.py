# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 13:53:02 2026

@author: MEAL ASSISTANT 1
"""

from pyspark.sql.functions import col, year

def clean_data(df):
    return (
        df
        .withColumn("year", year(col("transaction_date")))
        .filter(col("amount") > 0)
    )
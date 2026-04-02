# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 13:54:50 2026

@author: Walter Roye T. Fanka
"""

from pyspark.sql.functions import col, log1p

def add_features(df):
    return (
        df
        .withColumn("log_amount", log1p(col("amount")))
    )

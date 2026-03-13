# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 15:38:22 2026

@author: Walter Roye T. Fanka
"""
from pyspark.sql import DataFrame


def write_csv(df: DataFrame, output_path: str) -> None:
    if df.limit(1).count() == 0:
        raise ValueError("DataFrame is empty — nothing to write.")

    (
        df.write
        .mode("overwrite")
        .option("header", True)
	.partitionBy("year")
        .csv(output_path)
    )

    print(f"Data written to: {output_path}")

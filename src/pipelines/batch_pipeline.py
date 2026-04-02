# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 13:57:05 2026

@author: Walter Roye T. Fanka
"""
    
from src.utils.spark_session import get_spark_session
from src.ingestion.read_data import read_transactions
from src.transformations.clean_transform import clean_data
from src.validation.data_quality import validate_data
from src.output.write_data import write_csv


def run_pipeline(input_path: str, output_path: str):
    spark = get_spark_session("BatchTransactionPipeline")

    df_raw = read_transactions(spark, input_path)
    df_clean = clean_data(df_raw)

    # Deduplicate with incoming batch
    df_clean = df_clean.dropDuplicates(["transaction_id"])

    validate_data(df_clean)

    print("Preview rows:")
    df_clean.show(5, truncate=False)
    print("Row count:", df_clean.count())

    write_csv(df_clean, output_path)

    spark.stop()

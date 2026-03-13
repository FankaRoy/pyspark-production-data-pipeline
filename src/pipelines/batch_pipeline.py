# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 13:57:05 2026

@author: MEAL ASSISTANT 1
"""
    
from src.utils.spark_session import get_spark_session
from src.ingestion.read_data import read_transactions
from src.transformations.clean_transform import clean_data
from src.output.write_data import write_csv


# def run_pipeline(input_path: str, output_path: str):
#     spark = get_spark_session("BatchTransactionPipeline")

#     df_raw = read_transactions(spark, input_path)
#     df_clean = clean_data(df_raw)

#     # Debug visibility
#     print("Preview rows:")
#     df_clean.show(5, truncate=False)

#     print("Row count:", df_clean.count())

#     # write_parquet(df_clean, output_path)
#     write_csv(df_clean, "data/processed/transactions_csv")

#     spark.stop()


def run_pipeline(input_path: str, output_path: str):
    spark = get_spark_session("BatchTransactionPipeline")

    df_raw = read_transactions(spark, input_path)
    df_clean = clean_data(df_raw)

    print("Preview rows:")
    df_clean.show(5, truncate=False)
    print("Row count:", df_clean.count())

    write_csv(df_clean, output_path)

    spark.stop()

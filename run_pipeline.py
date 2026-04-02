


# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 14:40:50 2026

@author: Walter Roye T. Fanka
"""

from src.pipelines.batch_pipeline import run_pipeline

if __name__ == "__main__":
    run_pipeline(
        input_path="data/raw/synthetic_transactions.csv",
        output_path="data/processed/transactions_csv"
    )



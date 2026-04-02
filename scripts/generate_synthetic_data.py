# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 12:16:44 2026

@author: Walter Roye T. Fanka
"""

"""
Synthetic transaction dataset generator for the PySpark pipeline demo.
"""
from pathlib import Path
from datetime import datetime, timedelta
import random
import pandas as pd
import numpy as np


def get_next_transaction_id(output_file: Path) -> int:
    """
    Determine the next transaction_id by checking the existing raw dataset.
    """
    if output_file.exists():
        existing_df = pd.read_csv(output_file)
        if not existing_df.empty and "transaction_id" in existing_df.columns:
            return int(existing_df["transaction_id"].max()) + 1
    return 1


def generate_data(n: int = 10000, start_id: int = 1, batch_id: str = None) -> pd.DataFrame:
    np.random.seed(42)

    start_date = datetime.now() - timedelta(days=365)
    load_timestamp = datetime.now()

    if batch_id is None:
        batch_id = load_timestamp.strftime("batch_%Y%m%d_%H%M%S")

    data = {
        "transaction_id": range(start_id, start_id + n),
        "customer_id": np.random.randint(1000, 5000, n),
        "transaction_date": [
            start_date + timedelta(days=random.randint(0, 365))
            for _ in range(n)
        ],
        "amount": np.round(np.random.exponential(scale=200, size=n), 2),
        "currency": np.random.choice(["USD", "EUR", "GBP"], n),
        "channel": np.random.choice(["mobile", "web", "branch"], n),
        "country": np.random.choice(["AE", "UK", "US"], n),
        "is_fraud": np.random.choice([0, 1], n, p=[0.97, 0.03]),
        "batch_id": [batch_id] * n,
        "load_timestamp": [load_timestamp] * n,
    }

    return pd.DataFrame(data)


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[1]
    raw_dir = project_root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    output_file = raw_dir / "synthetic_transactions.csv"

    next_id = get_next_transaction_id(output_file)
    df_new = generate_data(n=10000, start_id=next_id)

    if output_file.exists():
        df_existing = pd.read_csv(output_file)
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_final = df_new

    df_final.to_csv(output_file, index=False)

    print(f"Incremental batch generated: {df_new['batch_id'].iloc[0]}")
    print(f"Rows added: {len(df_new)}")
    print(f"Transaction IDs: {df_new['transaction_id'].min()} to {df_new['transaction_id'].max()}")
    print(f"Updated raw dataset: {output_file}")


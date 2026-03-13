# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 12:16:44 2026

@author: MEAL ASSISTANT 1
"""

"""
Synthetic transaction dataset generator for the PySpark pipeline demo.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path


def generate_data(n=10000):
    np.random.seed(42)

    start_date = datetime.now() - timedelta(days=365)

    data = {
        "transaction_id": range(1, n + 1),
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
    }

    return pd.DataFrame(data)


if __name__ == "__main__":

    # project root = two folders above this script
    project_root = Path(__file__).resolve().parents[1]

    raw_dir = project_root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    output_file = raw_dir / "synthetic_transactions.csv"

    df = generate_data()
    df.to_csv(output_file, index=False)

    print(f"Synthetic dataset generated: {output_file}")
    print(f"Rows generated: {len(df)}")
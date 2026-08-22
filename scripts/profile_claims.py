from pathlib import Path

import pandas as pd


file_path = Path("data/raw/csv/claims.csv")

df = pd.read_csv(file_path)

print("=" * 80)
print("CLAIMS DATASET")
print("=" * 80)

print(f"\nShape: {df.shape}")

print("\nColumns:")
print(df.columns.tolist())

print("\nData types:")
print(df.dtypes)

print(f"\nDuplicate rows: {df.duplicated().sum()}")

print("\nNull counts:")
print(df.isna().sum())

print("\nSample rows:")
print(df.head(5).to_string(index=False))
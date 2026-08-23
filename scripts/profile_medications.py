from pathlib import Path

import pandas as pd


file_path = Path("data/raw/csv/medications.csv")

df = pd.read_csv(file_path)

print("=" * 80)
print("MEDICATIONS DATASET")
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


print("\n" + "=" * 80)
print("MEDICATION-SPECIFIC PROFILING")
print("=" * 80)

print(f"\nUnique patients: {df['PATIENT'].nunique()}")
print(f"Unique encounters: {df['ENCOUNTER'].nunique()}")
print(f"Unique medication codes: {df['CODE'].nunique()}")
print(f"Unique medication descriptions: {df['DESCRIPTION'].nunique()}")

print("\nSTOP status:")
print(f"Open-ended medications: {df['STOP'].isna().sum()}")
print(f"Completed medications: {df['STOP'].notna().sum()}")

df["START"] = pd.to_datetime(df["START"])
df["STOP"] = pd.to_datetime(df["STOP"])

duration_days = (df["STOP"] - df["START"]).dt.days

print("\nTreatment duration (days):")
print(duration_days.describe())

print(f"\nNegative durations: {(duration_days < 0).sum()}")
print(f"Zero-day durations: {(duration_days == 0).sum()}")

print("\nDispenses:")
print(df["DISPENSES"].describe())

print("\nCost:")
print(df[["BASE_COST", "PAYER_COVERAGE", "TOTALCOST"]].describe())

print("\nPotential grain duplicates:")
print(
    df.duplicated(
        subset=["PATIENT", "ENCOUNTER", "CODE", "START"]
    ).sum()
)

print("\nCost calculation check:")
expected_total = df["BASE_COST"] * df["DISPENSES"]

print(
    f"Rows where TOTALCOST != BASE_COST * DISPENSES: "
    f"{(df['TOTALCOST'].round(2) != expected_total.round(2)).sum()}"
)
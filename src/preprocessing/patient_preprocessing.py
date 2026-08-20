# 1. Import pandas
import pandas as pd


# 2. Define the patient preprocessing function
def preprocess_patients(df):

    # 3. Standardize column names
    # Example: " BIRTHDATE " → "birthdate"
    # Example: "HEALTHCARE_EXPENSES" → "healthcare_expenses"
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
    )

    # 4. Convert birthdate from string to datetime
    df["birthdate"] = pd.to_datetime(df["birthdate"])

    # 5. Convert deathdate from string to datetime
    # Missing death dates become NaT, which is expected.
    df["deathdate"] = pd.to_datetime(df["deathdate"])

    # 6. Create deceased flag
    # True  → deathdate exists
    # False → deathdate is missing
    df["is_deceased"] = df["deathdate"].notna()

    # 7. Return the processed DataFrame
    return df
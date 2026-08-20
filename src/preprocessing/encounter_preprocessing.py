# 1. Import pandas
import pandas as pd


# 2. Define encounter preprocessing function
def preprocess_encounters(df):

    # 3. Standardize column names
    df.columns = df.columns.str.lower().str.strip()

    # 4. Convert start and stop to datetime
    df["start"] = pd.to_datetime(df["start"])
    df["stop"] = pd.to_datetime(df["stop"])

    # 5. Calculate length of stay in days
    df["length_of_stay_days"] = (
        df["stop"] - df["start"]
    ).dt.total_seconds() / 86400

    # 6. Return the processed DataFrame
    return df
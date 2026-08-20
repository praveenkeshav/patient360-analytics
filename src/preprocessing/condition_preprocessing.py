import pandas as pd


def preprocess_conditions(df):

    # Standardize column names.
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
    )

    # Convert condition dates to datetime.
    df["start"] = pd.to_datetime(df["start"])
    df["stop"] = pd.to_datetime(df["stop"])

    # Calculate how long the condition was recorded.
    df["condition_duration_days"] = (
        (df["stop"] - df["start"])
        .dt.total_seconds()
        / 86400
    )

    return df
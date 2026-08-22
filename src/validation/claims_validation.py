def validate_claims(df):
    required_columns = [
        "claim_id",
        "patient_id",
        "provider_id",
        "service_date",
        "_ingested_at",
        "_ingestion_date",
        "_source_file",
    ]

    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    if df["claim_id"].isna().any():
        raise ValueError("claim_id contains null values")

    if df["claim_id"].duplicated().any():
        raise ValueError("claim_id contains duplicates")

    if df["patient_id"].isna().any():
        raise ValueError("patient_id contains null values")

    if df["_ingested_at"].isna().any():
        raise ValueError("_ingested_at contains null values")

    if df["_ingestion_date"].isna().any():
        raise ValueError("_ingestion_date contains null values")

    return True
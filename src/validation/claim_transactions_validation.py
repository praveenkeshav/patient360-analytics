def validate_claim_transactions(df):
    required_columns = [
        "transaction_id",
        "claim_id",
        "patient_id",
        "transaction_type",
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

    if df["transaction_id"].isna().any():
        raise ValueError("transaction_id contains null values")

    if df["transaction_id"].duplicated().any():
        raise ValueError("transaction_id contains duplicates")

    if df["claim_id"].isna().any():
        raise ValueError("claim_id contains null values")

    if df["patient_id"].isna().any():
        raise ValueError("patient_id contains null values")

    valid_types = {
        "PAYMENT",
        "CHARGE",
        "TRANSFERIN",
        "TRANSFEROUT",
    }

    invalid_types = set(df["transaction_type"].dropna().unique()) - valid_types

    if invalid_types:
        raise ValueError(f"Invalid transaction types: {invalid_types}")

    if df["_ingested_at"].isna().any():
        raise ValueError("_ingested_at contains null values")

    if df["_ingestion_date"].isna().any():
        raise ValueError("_ingestion_date contains null values")

    return True
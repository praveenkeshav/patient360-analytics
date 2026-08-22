import pandas as pd
import pytest

from src.validation.claim_transactions_validation import (
    validate_claim_transactions,
)


def valid_transactions_df():
    return pd.DataFrame({
        "transaction_id": ["T1", "T2"],
        "claim_id": ["C1", "C2"],
        "patient_id": ["P1", "P2"],
        "transaction_type": ["CHARGE", "PAYMENT"],
        "_ingested_at": ["2026-08-22", "2026-08-22"],
        "_ingestion_date": ["2026-08-22", "2026-08-22"],
        "_source_file": [
            "claims_transactions.csv",
            "claims_transactions.csv",
        ],
    })


def test_valid_claim_transactions():
    df = valid_transactions_df()

    assert validate_claim_transactions(df) is True


def test_transaction_id_null():
    df = valid_transactions_df()
    df.loc[0, "transaction_id"] = None

    with pytest.raises(ValueError, match="transaction_id contains null"):
        validate_claim_transactions(df)


def test_transaction_id_duplicate():
    df = valid_transactions_df()
    df.loc[1, "transaction_id"] = "T1"

    with pytest.raises(ValueError, match="transaction_id contains duplicates"):
        validate_claim_transactions(df)


def test_invalid_transaction_type():
    df = valid_transactions_df()
    df.loc[0, "transaction_type"] = "INVALID"

    with pytest.raises(ValueError, match="Invalid transaction types"):
        validate_claim_transactions(df)


def test_missing_column():
    df = valid_transactions_df().drop(columns=["claim_id"])

    with pytest.raises(ValueError, match="Missing columns"):
        validate_claim_transactions(df)
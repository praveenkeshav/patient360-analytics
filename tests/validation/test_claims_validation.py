import pandas as pd
import pytest

from src.validation.claims_validation import validate_claims


def valid_claims_df():
    return pd.DataFrame({
        "claim_id": ["C1", "C2"],
        "patient_id": ["P1", "P2"],
        "provider_id": ["PR1", "PR2"],
        "service_date": ["2024-01-01", "2024-01-02"],
        "_ingested_at": ["2026-08-22", "2026-08-22"],
        "_ingestion_date": ["2026-08-22", "2026-08-22"],
        "_source_file": ["claims.csv", "claims.csv"],
    })


def test_valid_claims():
    df = valid_claims_df()

    assert validate_claims(df) is True


def test_claim_id_null():
    df = valid_claims_df()
    df.loc[0, "claim_id"] = None

    with pytest.raises(ValueError, match="claim_id contains null"):
        validate_claims(df)


def test_claim_id_duplicate():
    df = valid_claims_df()
    df.loc[1, "claim_id"] = "C1"

    with pytest.raises(ValueError, match="claim_id contains duplicates"):
        validate_claims(df)


def test_patient_id_null():
    df = valid_claims_df()
    df.loc[0, "patient_id"] = None

    with pytest.raises(ValueError, match="patient_id contains null"):
        validate_claims(df)


def test_missing_column():
    df = valid_claims_df().drop(columns=["service_date"])

    with pytest.raises(ValueError, match="Missing columns"):
        validate_claims(df)
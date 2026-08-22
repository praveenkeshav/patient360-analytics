import pandas as pd

from src.preprocessing.claim_transactions_preprocessing import (
    preprocess_claim_transactions,
)


def test_claim_transaction_columns():
    df = pd.DataFrame({
        "ID": ["T1"],
        "CLAIMID": ["C1"],
        "CHARGEID": ["CH1"],
        "PATIENTID": ["P1"],
        "TYPE": ["CHARGE"],
        "FROMDATE": ["2024-01-01"],
        "TODATE": ["2024-01-01"],
    })

    result = preprocess_claim_transactions(df)

    assert "transaction_id" in result.columns
    assert "claim_id" in result.columns
    assert "charge_id" in result.columns
    assert "patient_id" in result.columns
    assert "transaction_type" in result.columns


def test_claim_transaction_ingestion_metadata():
    df = pd.DataFrame({
        "ID": ["T1"],
        "CLAIMID": ["C1"],
        "CHARGEID": ["CH1"],
        "PATIENTID": ["P1"],
        "TYPE": ["CHARGE"],
        "FROMDATE": ["2024-01-01"],
        "TODATE": ["2024-01-01"],
    })

    result = preprocess_claim_transactions(df)

    assert result["_ingested_at"].notna().all()
    assert result["_ingestion_date"].notna().all()
    assert result["_source_file"].eq("claims_transactions.csv").all()
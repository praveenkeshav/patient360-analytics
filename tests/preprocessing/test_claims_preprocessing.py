import pandas as pd

from src.preprocessing.claims_preprocessing import preprocess_claims


def test_claims_columns():
    df = pd.DataFrame({
        "Id": ["C1"],
        "PATIENTID": ["P1"],
        "PROVIDERID": ["PR1"],
        "SERVICEDATE": ["2024-01-01"],
        "CURRENTILLNESSDATE": ["2024-01-01"],
        "LASTBILLEDDATE1": ["2024-01-01"],
        "LASTBILLEDDATE2": ["2024-01-01"],
        "LASTBILLEDDATEP": ["2024-01-01"],
    })

    result = preprocess_claims(df)

    assert "claim_id" in result.columns
    assert "patient_id" in result.columns
    assert "provider_id" in result.columns
    assert "service_date" in result.columns


def test_claims_ingestion_metadata():
    df = pd.DataFrame({
        "Id": ["C1"],
        "PATIENTID": ["P1"],
        "PROVIDERID": ["PR1"],
        "CURRENTILLNESSDATE": ["2024-01-01"],
        "SERVICEDATE": ["2024-01-01"],
        "LASTBILLEDDATE1": ["2024-01-01"],
        "LASTBILLEDDATE2": ["2024-01-01"],
        "LASTBILLEDDATEP": ["2024-01-01"],
    })

    result = preprocess_claims(df)

    assert result["_ingested_at"].notna().all()
    assert result["_ingestion_date"].notna().all()
    assert result["_source_file"].eq("claims.csv").all()
import pandas as pd

from src.preprocessing.fhir_parser import (
    classify_lab_abnormality,
    parse_fhir_bundle,
)


FHIR_FILE = (
    "data/raw/fhir/"
    "Abdul218_Harris789_b0a06ead-cc42-aa48-dad6-841d4aa679fa.json"
)


def test_parse_fhir_bundle_returns_laboratory_observations():
    df = parse_fhir_bundle(FHIR_FILE)

    assert not df.empty
    assert "observation_id" in df.columns
    assert "patient_id" in df.columns
    assert "loinc_code" in df.columns
    assert "value" in df.columns


def test_classify_lab_abnormality():
    observations = pd.DataFrame(
        [
            {
                "observation_id": "obs-1",
                "patient_id": "patient-1",
                "encounter_id": "enc-1",
                "observation_date": "2026-08-19",
                "loinc_code": "2947-0",
                "observation_name": "Sodium",
                "value": 150,
                "unit": "mmol/L",
            }
        ]
    )

    reference = pd.DataFrame(
        [
            {
                "loinc_code": "2947-0",
                "lab_name": "Sodium",
                "unit": "mmol/L",
                "normal_low": 136,
                "normal_high": 144,
            }
        ]
    )

    result = classify_lab_abnormality(observations, reference)

    assert result.iloc[0]["is_abnormal"] == True
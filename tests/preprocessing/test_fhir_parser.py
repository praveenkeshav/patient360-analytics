# 1. Import pandas for creating test DataFrames
import pandas as pd


# 2. Import the functions we want to test
from src.preprocessing.fhir_parser import (
    classify_lab_abnormality,
    parse_all_fhir_files,
    parse_fhir_bundle,
)


# 3. Define one test FHIR fixture for the parser test
FHIR_FILE = "tests/fixtures/fhir/sample_bundle.json"

# 4. Test that the FHIR parser extracts laboratory observations
def test_parse_fhir_bundle_returns_laboratory_observations():

    # Arrange - use the sample FHIR file
    file_path = FHIR_FILE

    # Act - parse the FHIR Bundle
    result = parse_fhir_bundle(file_path)

    # Assert - make sure laboratory observations were extracted
    assert not result.empty

    # Assert - required columns exist
    assert "observation_id" in result.columns
    assert "patient_id" in result.columns
    assert "loinc_code" in result.columns
    assert "value" in result.columns
    assert "value_text" in result.columns


# 5. Test that all FHIR files can be parsed
def test_parse_all_fhir_files():

    # Arrange - use the test FHIR fixture directory
    folder = "tests/fixtures/fhir"

    # Act - parse all FHIR files
    result = parse_all_fhir_files(folder)

    # Assert - laboratory observations were extracted
    assert not result.empty

    # Assert - required columns exist
    assert "observation_id" in result.columns
    assert "patient_id" in result.columns
    assert "loinc_code" in result.columns
    assert "value" in result.columns


# 6. Test that abnormal laboratory values are identified correctly
def test_classify_lab_abnormality():

    # Arrange - create one laboratory observation
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

    # Arrange - create the normal reference range
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

    # Act - classify the laboratory result
    result = classify_lab_abnormality(
        observations,
        reference,
    )

    # Assert - 150 is above the normal high of 144
    assert result.iloc[0]["is_abnormal"]
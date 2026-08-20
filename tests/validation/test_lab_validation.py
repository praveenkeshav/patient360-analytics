import pandas as pd

# Import the laboratory validation function we want to test.
from src.validation.lab_validation import validate_labs


# Test that valid laboratory data passes validation.
def test_valid_laboratory_data():

    # Arrange - create one numeric laboratory result
    # and one qualitative laboratory result.
    observations = pd.DataFrame(
        [
            {
                "observation_id": "obs-1",
                "patient_id": "patient-1",
                "encounter_id": "enc-1",
                "loinc_code": "2947-0",
                "value": 140,
                "value_text": None,
            },
            {
                "observation_id": "obs-2",
                "patient_id": "patient-2",
                "encounter_id": "enc-2",
                "loinc_code": "65750-2",
                "value": None,
                "value_text": "Negative",
            },
        ]
    )

    # Arrange - define valid patient IDs.
    patient_ids = {"patient-1", "patient-2"}

    # Act and Assert - valid data should pass validation.
    assert validate_labs(
        observations,
        patient_ids,
    ) is True


# Test that duplicate observation IDs are rejected.
def test_duplicate_observation_ids_fail():

    # Arrange - create two observations with the same ID.
    observations = pd.DataFrame(
        [
            {
                "observation_id": "obs-1",
                "patient_id": "patient-1",
                "encounter_id": "enc-1",
                "loinc_code": "2947-0",
                "value": 140,
                "value_text": None,
            },
            {
                "observation_id": "obs-1",
                "patient_id": "patient-1",
                "encounter_id": "enc-1",
                "loinc_code": "2947-0",
                "value": 141,
                "value_text": None,
            },
        ]
    )

    # Assert - duplicate observation IDs should fail validation.
    assert validate_labs(
        observations,
        {"patient-1"},
    ) is False


# Test that an observation with no result is rejected.
def test_missing_both_values_fail():

    # Arrange - both numeric and text results are missing.
    observations = pd.DataFrame(
        [
            {
                "observation_id": "obs-1",
                "patient_id": "patient-1",
                "encounter_id": "enc-1",
                "loinc_code": "2947-0",
                "value": None,
                "value_text": None,
            }
        ]
    )

    # Assert - every observation must have a numeric
    # value or a qualitative text value.
    assert validate_labs(
        observations,
        {"patient-1"},
    ) is False


# Test that an unknown patient ID is rejected.
def test_unknown_patient_fails():

    # Arrange - the observation contains a patient ID
    # that does not exist in the patient reference set.
    observations = pd.DataFrame(
        [
            {
                "observation_id": "obs-1",
                "patient_id": "unknown-patient",
                "encounter_id": "enc-1",
                "loinc_code": "2947-0",
                "value": 140,
                "value_text": None,
            }
        ]
    )

    # Assert - patient referential integrity should fail.
    assert validate_labs(
        observations,
        {"patient-1"},
    ) is False
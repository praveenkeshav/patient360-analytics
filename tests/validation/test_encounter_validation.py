# 1. Import pandas
import pandas as pd
import pytest


# 2. Import validation function
from src.validation.encounter_validation import validate_encounters


# 3. Test valid encounter data
def test_validate_encounters_valid_data():

    # 4. ARRANGE
    df = pd.DataFrame({
        "id": [1, 2],
        "start": pd.to_datetime([
            "2025-01-01 10:00:00",
            "2025-01-05 08:00:00",
        ]),
        "stop": pd.to_datetime([
            "2025-01-02 10:00:00",
            "2025-01-05 20:00:00",
        ]),
        "patient": ["P1", "P2"],
    })

    patient_ids = {"P1", "P2"}

    # 5. ACT + ASSERT
    assert validate_encounters(df, patient_ids) is True


# 6. Test null encounter ID
def test_validate_encounters_null_id():

    df = pd.DataFrame({
        "id": [1, None],
        "start": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "stop": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "patient": ["P1", "P2"],
    })

    with pytest.raises(ValueError, match="Encounter ID contains null values"):
        validate_encounters(df, {"P1", "P2"})


# 7. Test duplicate encounter ID
def test_validate_encounters_duplicate_id():

    df = pd.DataFrame({
        "id": [1, 1],
        "start": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "stop": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "patient": ["P1", "P2"],
    })

    with pytest.raises(ValueError, match="Duplicate encounter IDs found"):
        validate_encounters(df, {"P1", "P2"})


# 8. Test null start date
def test_validate_encounters_null_start():

    df = pd.DataFrame({
        "id": [1, 2],
        "start": pd.to_datetime(["2025-01-01", None]),
        "stop": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "patient": ["P1", "P2"],
    })

    with pytest.raises(ValueError, match="Start date contains null values"):
        validate_encounters(df, {"P1", "P2"})


# 9. Test null stop date
def test_validate_encounters_null_stop():

    df = pd.DataFrame({
        "id": [1, 2],
        "start": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "stop": pd.to_datetime(["2025-01-01", None]),
        "patient": ["P1", "P2"],
    })

    with pytest.raises(ValueError, match="Stop date contains null values"):
        validate_encounters(df, {"P1", "P2"})


# 10. Test invalid date order
def test_validate_encounters_invalid_dates():

    df = pd.DataFrame({
        "id": [1, 2],
        "start": pd.to_datetime(["2025-01-01", "2025-01-05"]),
        "stop": pd.to_datetime(["2025-01-01", "2025-01-04"]),
        "patient": ["P1", "P2"],
    })

    with pytest.raises(
        ValueError,
        match="Stop date cannot be before start date"
    ):
        validate_encounters(df, {"P1", "P2"})


# 11. Test invalid patient reference
def test_validate_encounters_invalid_patient():

    df = pd.DataFrame({
        "id": [1, 2],
        "start": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "stop": pd.to_datetime(["2025-01-01", "2025-01-02"]),
        "patient": ["P1", "P999"],
    })

    with pytest.raises(
        ValueError,
        match="Encounter contains invalid patient ID"
    ):
        validate_encounters(df, {"P1", "P2"})
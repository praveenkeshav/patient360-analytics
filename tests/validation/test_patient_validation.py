# 1. Import pandas
import pandas as pd
import pytest


# 2. Import the validation function
from src.validation.patient_validation import validate_patients


# 3. Test valid patient data
def test_validate_patients_valid_data():

    # 4. ARRANGE
    # Create valid patient records
    df = pd.DataFrame({
        "id": [1, 2],
        "birthdate": pd.to_datetime([
            "1990-01-01",
            "1985-05-10",
        ]),
        "deathdate": pd.to_datetime([
            None,
            "2020-01-01",
        ]),
    })

    # 5. ACT + ASSERT
    # Validation should successfully return True
    assert validate_patients(df) is True

    # 6. Test null patient ID
def test_validate_patients_null_id():

    # ARRANGE
    df = pd.DataFrame({
        "id": [1, None],
        "birthdate": pd.to_datetime([
            "1990-01-01",
            "1985-05-10",
        ]),
        "deathdate": pd.to_datetime([
            None,
            None,
        ]),
    })

    # ACT + ASSERT
    with pytest.raises(ValueError, match="Patient ID contains null values"):
        validate_patients(df)


# 7. Test duplicate patient ID
def test_validate_patients_duplicate_id():

    # ARRANGE
    df = pd.DataFrame({
        "id": [1, 1],
        "birthdate": pd.to_datetime([
            "1990-01-01",
            "1985-05-10",
        ]),
        "deathdate": pd.to_datetime([
            None,
            None,
        ]),
    })

    # ACT + ASSERT
    with pytest.raises(ValueError, match="Duplicate patient IDs found"):
        validate_patients(df)


# 8. Test null birthdate
def test_validate_patients_null_birthdate():

    # ARRANGE
    df = pd.DataFrame({
        "id": [1, 2],
        "birthdate": pd.to_datetime([
            "1990-01-01",
            None,
        ]),
        "deathdate": pd.to_datetime([
            None,
            None,
        ]),
    })

    # ACT + ASSERT
    with pytest.raises(ValueError, match="Birthdate contains null values"):
        validate_patients(df)


# 9. Test invalid deathdate
def test_validate_patients_invalid_dates():

    # ARRANGE
    df = pd.DataFrame({
        "id": [1, 2],
        "birthdate": pd.to_datetime([
            "1990-01-01",
            "1985-05-10",
        ]),
        "deathdate": pd.to_datetime([
            None,
            "1980-01-01",
        ]),
    })

    # ACT + ASSERT
    with pytest.raises(
        ValueError,
        match="Deathdate cannot be before birthdate"
    ):
        validate_patients(df)
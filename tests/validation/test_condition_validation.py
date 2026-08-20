import pandas as pd
import pytest

from src.validation.condition_validation import validate_conditions


# Create a small valid Conditions DataFrame.
# This DataFrame is reused by the validation tests.
def create_valid_conditions():

    return pd.DataFrame({
        "start": pd.to_datetime(["2024-01-01"]),
        "stop": pd.to_datetime(["2024-01-11"]),
        "patient": ["P1"],
        "encounter": ["E1"],
        "code": ["123"],
    })


# Test that valid condition data passes all validation rules.
def test_valid_conditions():

    df = create_valid_conditions()

    assert validate_conditions(
        df,
        {"P1"},
        {"E1"},
    )


# Test that validation fails when the patient ID
# does not exist in the valid patient ID set.
def test_invalid_patient():

    df = create_valid_conditions()

    with pytest.raises(ValueError):
        validate_conditions(
            df,
            {"P2"},
            {"E1"},
        )


# Test that validation fails when the encounter ID
# does not exist in the valid encounter ID set.
def test_invalid_encounter():

    df = create_valid_conditions()

    with pytest.raises(ValueError):
        validate_conditions(
            df,
            {"P1"},
            {"E2"},
        )


# Test that validation fails when the condition stop date
# occurs before the condition start date.
def test_stop_before_start():

    df = create_valid_conditions()

    # Change the stop date to an earlier date.
    df.loc[0, "stop"] = pd.Timestamp("2023-12-31")

    with pytest.raises(ValueError):
        validate_conditions(
            df,
            {"P1"},
            {"E1"},
        )


# Test that validation fails when the condition code is missing.
def test_missing_condition_code():

    df = create_valid_conditions()

    # Set the condition code to null.
    df.loc[0, "code"] = None

    with pytest.raises(ValueError):
        validate_conditions(
            df,
            {"P1"},
            {"E1"},
        )


# Test that validation fails when the condition start date is missing.
def test_missing_start():

    df = create_valid_conditions()

    # Set the condition start date to NaT.
    df.loc[0, "start"] = pd.NaT

    with pytest.raises(ValueError):
        validate_conditions(
            df,
            {"P1"},
            {"E1"},
        )
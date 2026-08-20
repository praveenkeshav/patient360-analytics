import pandas as pd

from src.preprocessing.condition_preprocessing import preprocess_conditions


# Test the Conditions preprocessing function.
def test_condition_preprocessing():

    # Create a small sample Conditions DataFrame
    # using the original Synthea-style column names.
    df = pd.DataFrame({
        "START": ["2024-01-01"],
        "STOP": ["2024-01-11"],
        "PATIENT": ["P1"],
        "ENCOUNTER": ["E1"],
        "CODE": ["123"],
        "DESCRIPTION": ["Test condition"],
    })

    # Apply the Conditions preprocessing function.
    result = preprocess_conditions(df)

    # Check that column names were standardized
    # and the duration column was created.
    assert "start" in result.columns
    assert "stop" in result.columns
    assert "condition_duration_days" in result.columns

    # Check that the start column was converted to datetime.
    assert pd.api.types.is_datetime64_any_dtype(
        result["start"]
    )

    # Check that the stop column was converted to datetime.
    assert pd.api.types.is_datetime64_any_dtype(
        result["stop"]
    )

    # Check that the condition duration was calculated correctly.
    # 2024-01-01 to 2024-01-11 = 10 days.
    assert result.loc[0, "condition_duration_days"] == 10
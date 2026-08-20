# 1. Import pandas
import pandas as pd


# 2. Import the function we want to test
from src.preprocessing.patient_preprocessing import preprocess_patients


# 3. Define the test
def test_preprocess_patients_basic():

    # 4. ARRANGE
    # Create a small, realistic input DataFrame
    df = pd.DataFrame({
        " ID ": [1, 2],
        " BIRTHDATE ": ["1990-01-01", "1985-05-10"],
        " DEATHDATE ": [None, "2020-01-01"],
    })

    # 5. ACT
    # Run our preprocessing function
    result = preprocess_patients(df)

    # 6. ASSERT — check required columns
    expected_cols = {
        "id",
        "birthdate",
        "deathdate",
        "is_deceased",
    }

    assert expected_cols.issubset(set(result.columns))

    # 7. ASSERT — check date data types
    assert pd.api.types.is_datetime64_any_dtype(
        result["birthdate"]
    )

    assert pd.api.types.is_datetime64_any_dtype(
        result["deathdate"]
    )

    # 8. ASSERT — check business logic
    # Patient 1 has no deathdate → False
    # Patient 2 has deathdate → True
    assert result["is_deceased"].tolist() == [False, True]

    # 9. ASSERT — check missing-value handling
    assert pd.isna(result.loc[0, "deathdate"])

    # 10. ASSERT — check actual death date
    assert result.loc[1, "deathdate"].year == 2020
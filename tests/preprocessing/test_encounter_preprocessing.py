# 1. Import pandas
import pandas as pd


# 2. Import preprocessing function
from src.preprocessing.encounter_preprocessing import preprocess_encounters


# 3. Test basic encounter preprocessing
def test_preprocess_encounters_basic():

    # 4. ARRANGE
    # Create two simple encounter records
    df = pd.DataFrame({
        " ID ": [1, 2],
        " START ": [
            "2025-01-01 10:00:00",
            "2025-01-05 08:00:00",
        ],
        " STOP ": [
            "2025-01-02 10:00:00",
            "2025-01-05 20:00:00",
        ],
    })

    # 5. ACT
    result = preprocess_encounters(df)

    # 6. ASSERT - column names
    expected_cols = {
        "id",
        "start",
        "stop",
        "length_of_stay_days",
    }

    assert expected_cols.issubset(set(result.columns))

    # 7. ASSERT - data types
    assert pd.api.types.is_datetime64_any_dtype(result["start"])
    assert pd.api.types.is_datetime64_any_dtype(result["stop"])

    # 8. ASSERT - business logic
    assert result.loc[0, "length_of_stay_days"] == 1.0
    assert result.loc[1, "length_of_stay_days"] == 0.5
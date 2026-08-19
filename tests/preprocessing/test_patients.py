import pandas as pd

from src.preprocessing.patients import preprocess_patients


def test_preprocess_patients():
    df = pd.DataFrame({
        " ID ": [1],
        " First ": ["John"],
    })

    result = preprocess_patients(df)

    assert list(result.columns) == ["id", "first"]
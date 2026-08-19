from src.preprocessing.patients import preprocess_patients


def test_preprocess_patients():
    df = preprocess_patients("data/raw/csv/patients.csv")

    assert len(df) == 1163
    assert all(col == col.lower() for col in df.columns)
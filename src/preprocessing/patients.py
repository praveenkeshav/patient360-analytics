import pandas as pd

def preprocess_patients(file_path):
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.lower().str.strip()
    return df
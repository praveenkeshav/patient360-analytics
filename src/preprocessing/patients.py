import pandas as pd

def preprocess_patients(df):
    df.columns = df.columns.str.lower().str.strip()
    return df
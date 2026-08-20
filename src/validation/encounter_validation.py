# 1. Define encounter validation function
def validate_encounters(df, patient_ids):

    # 2. Check encounter IDs are not missing
    if df["id"].isna().any():
        raise ValueError("Encounter ID contains null values")

    # 3. Check encounter IDs are unique
    if df["id"].duplicated().any():
        raise ValueError("Duplicate encounter IDs found")

    # 4. Check start dates are not missing
    if df["start"].isna().any():
        raise ValueError("Start date contains null values")

    # 5. Check stop dates are not missing
    if df["stop"].isna().any():
        raise ValueError("Stop date contains null values")

    # 6. Check date logic
    if (df["stop"] < df["start"]).any():
        raise ValueError("Stop date cannot be before start date")

    # 7. Check patient references
    if (~df["patient"].isin(patient_ids)).any():
        raise ValueError("Encounter contains invalid patient ID")

    # 8. All validation rules passed
    return True
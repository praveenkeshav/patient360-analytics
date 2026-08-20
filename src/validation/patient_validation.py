# 1. Define patient validation function
def validate_patients(df):

    # 2. Check patient IDs are not missing
    if df["id"].isna().any():
        raise ValueError("Patient ID contains null values")

    # 3. Check patient IDs are unique
    if df["id"].duplicated().any():
        raise ValueError("Duplicate patient IDs found")

    # 4. Check birthdates are not missing
    if df["birthdate"].isna().any():
        raise ValueError("Birthdate contains null values")

    # 5. Check date logic
    if (df["deathdate"] < df["birthdate"]).any():
        raise ValueError("Deathdate cannot be before birthdate")

    # 6. All validation rules passed
    return True
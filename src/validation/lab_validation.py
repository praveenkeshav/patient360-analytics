# Validate laboratory observation data.
def validate_labs(observations_df, patient_ids):

    # Observation IDs must be unique.
    if observations_df["observation_id"].duplicated().any():
        return False

    # Required identifiers must not be missing.
    if observations_df["observation_id"].isna().any():
        return False

    if observations_df["patient_id"].isna().any():
        return False

    if observations_df["encounter_id"].isna().any():
        return False

    if observations_df["loinc_code"].isna().any():
        return False

    # Every observation must have either a numeric
    # result or a text result.
    if (
        observations_df["value"].isna()
        & observations_df["value_text"].isna()
    ).any():
        return False

    # Patient IDs must exist in patients.csv.
    if not observations_df["patient_id"].isin(patient_ids).all():
        return False

    return True
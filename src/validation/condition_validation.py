def validate_conditions(df, patient_ids, encounter_ids):

    # Condition start date cannot be null.
    if df["start"].isna().any():
        raise ValueError("Condition start date contains null values")

    # Condition stop date cannot be before start date.
    if (df["stop"] < df["start"]).any():
        raise ValueError(
            "Condition stop date cannot be before start date"
        )

    # Every condition must reference a valid patient.
    if (~df["patient"].isin(patient_ids)).any():
        raise ValueError(
            "Condition contains invalid patient ID"
        )

    # Every condition must reference a valid encounter.
    if (~df["encounter"].isin(encounter_ids)).any():
        raise ValueError(
            "Condition contains invalid encounter ID"
        )

    # Condition code cannot be null.
    if df["code"].isna().any():
        raise ValueError(
            "Condition code contains null values"
        )

    return True
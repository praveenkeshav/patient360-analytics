import json
from pathlib import Path

import pandas as pd


# 1. Parse one FHIR JSON file
def parse_fhir_bundle(file_path):

    # Open the JSON file and load it into a Python dictionary.
    with open(file_path, encoding="utf-8") as file:
        bundle = json.load(file)

    records = []

    # Look through every resource inside the FHIR Bundle.
    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})

        # We only want Observation resources.
        if resource.get("resourceType") != "Observation":
            continue

        # Check whether the Observation belongs to the laboratory category.
        is_laboratory = False

        for category in resource.get("category", []):
            for coding in category.get("coding", []):
                if coding.get("code") == "laboratory":
                    is_laboratory = True

        # Skip non-laboratory observations.
        if not is_laboratory:
            continue

        # Get the first coding from the Observation code.
        coding = resource.get("code", {}).get("coding", [{}])[0]

        # Get a numeric laboratory value when available.
        value_quantity = resource.get("valueQuantity", {})
        value = value_quantity.get("value")

        # Get a text result when the Observation uses valueCodeableConcept.
        value_text = None

        value_concept = resource.get("valueCodeableConcept", {})

        if value_concept:
            value_text = value_concept.get("text")

            if not value_text:
                concept_coding = value_concept.get("coding", [{}])[0]
                value_text = concept_coding.get("display")

        # Get a text result when the Observation uses valueString.
        if value_text is None:
            value_text = resource.get("valueString")

        # Create one row for the laboratory observation.
        records.append(
            {
                "observation_id": resource.get("id"),
                "patient_id": resource.get("subject", {})
                .get("reference", "")
                .replace("urn:uuid:", ""),
                "encounter_id": resource.get("encounter", {})
                .get("reference", "")
                .replace("urn:uuid:", ""),
                "observation_date": resource.get("effectiveDateTime"),
                "loinc_code": coding.get("code"),
                "observation_name": coding.get("display"),
                "value": value,
                "value_text": value_text,
                "unit": value_quantity.get("unit"),
            }
        )

    # Convert the extracted records into a DataFrame.
    return pd.DataFrame(records)


# 2. Parse all FHIR files in a directory
def parse_all_fhir_files(folder_path):

    # Find every JSON file in the FHIR directory.
    files = Path(folder_path).glob("*.json")

    all_records = []

    # Process each FHIR file.
    for file_path in files:
        result = parse_fhir_bundle(file_path)

        if not result.empty:
            all_records.append(result)

    # Combine results from all patients.
    if not all_records:
        return pd.DataFrame()

    return pd.concat(all_records, ignore_index=True)


# 3. Classify laboratory observations as abnormal
def classify_lab_abnormality(observations_df, reference_df):

    # Keep only the reference columns needed for the comparison.
    reference = reference_df[
        [
            "loinc_code",
            "lab_name",
            "unit",
            "normal_low",
            "normal_high",
        ]
    ]

    # Match each observation with its laboratory reference range.
    result = observations_df.merge(
        reference,
        on="loinc_code",
        how="left",
        suffixes=("", "_reference"),
    )

    # Mark the result abnormal when it is below or above the reference range.
    result["is_abnormal"] = (
        result["normal_low"].notna()
        & result["normal_high"].notna()
        & (
            (result["value"] < result["normal_low"])
            | (result["value"] > result["normal_high"])
        )
    )

    return result
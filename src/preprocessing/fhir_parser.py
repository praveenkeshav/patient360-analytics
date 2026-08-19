import json

import pandas as pd


def parse_fhir_bundle(file_path):
    """Extract laboratory observations from a FHIR Bundle."""

    with open(file_path, encoding="utf-8") as file:
        bundle = json.load(file)

    records = []

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})

        if resource.get("resourceType") != "Observation":
            continue

        categories = resource.get("category", [])
        is_laboratory = any(
            coding.get("code") == "laboratory"
            for category in categories
            for coding in category.get("coding", [])
        )

        if not is_laboratory:
            continue

        coding = resource.get("code", {}).get("coding", [{}])[0]

        value_quantity = resource.get("valueQuantity", {})

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
                "value": value_quantity.get("value"),
                "unit": value_quantity.get("unit"),
            }
        )

    return pd.DataFrame(records)


def classify_lab_abnormality(observations_df, reference_df):
    """Match laboratory observations to reference ranges."""

    result = observations_df.merge(
        reference_df[
            [
                "loinc_code",
                "lab_name",
                "unit",
                "normal_low",
                "normal_high",
            ]
        ],
        on="loinc_code",
        how="left",
        suffixes=("", "_reference"),
    )

    result["is_abnormal"] = (
        result["normal_low"].notna()
        & result["normal_high"].notna()
        & (
            (result["value"] < result["normal_low"])
            | (result["value"] > result["normal_high"])
        )
    )

    return result
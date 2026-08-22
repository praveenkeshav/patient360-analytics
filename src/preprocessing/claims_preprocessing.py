import pandas as pd


def preprocess_claims(df, source_file="claims.csv"):

    # Standardize column names
    df.columns = df.columns.str.lower().str.strip()

    # Rename columns
    df = df.rename(columns={
        "id": "claim_id",
        "patientid": "patient_id",
        "providerid": "provider_id",
        "primarypatientinsuranceid": "primary_insurance_id",
        "secondarypatientinsuranceid": "secondary_insurance_id",
        "departmentid": "department_id",
        "patientdepartmentid": "patient_department_id",
        "diagnosis1": "diagnosis_code_1",
        "diagnosis2": "diagnosis_code_2",
        "diagnosis3": "diagnosis_code_3",
        "diagnosis4": "diagnosis_code_4",
        "diagnosis5": "diagnosis_code_5",
        "diagnosis6": "diagnosis_code_6",
        "diagnosis7": "diagnosis_code_7",
        "diagnosis8": "diagnosis_code_8",
        "referringproviderid": "referring_provider_id",
        "appointmentid": "appointment_id",
        "currentillnessdate": "current_illness_date",
        "servicedate": "service_date",
        "supervisingproviderid": "supervising_provider_id",
        "status1": "status_1",
        "status2": "status_2",
        "statusp": "status_p",
        "outstanding1": "outstanding_1",
        "outstanding2": "outstanding_2",
        "outstandingp": "outstanding_p",
        "lastbilleddate1": "last_billed_date_1",
        "lastbilleddate2": "last_billed_date_2",
        "lastbilleddatep": "last_billed_date_p",
        "healthcareclaimtypeid1": "healthcare_claim_type_id_1",
        "healthcareclaimtypeid2": "healthcare_claim_type_id_2",
    })

    # Convert dates
    date_columns = [
        "current_illness_date",
        "service_date",
        "last_billed_date_1",
        "last_billed_date_2",
        "last_billed_date_p",
    ]

    for column in date_columns:
        df[column] = pd.to_datetime(df[column], errors="coerce", utc=True)

    # Add ingestion metadata
    ingested_at = pd.Timestamp.now(tz="UTC")

    df["_ingested_at"] = ingested_at
    df["_ingestion_date"] = ingested_at.date()
    df["_source_file"] = source_file

    return df
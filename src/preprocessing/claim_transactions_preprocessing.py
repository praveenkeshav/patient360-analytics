import pandas as pd


def preprocess_claim_transactions(
    df,
    source_file="claims_transactions.csv",
):

    # Standardize column names
    df.columns = df.columns.str.lower().str.strip()

    # Rename columns
    df = df.rename(columns={
        "id": "transaction_id",
        "claimid": "claim_id",
        "chargeid": "charge_id",
        "patientid": "patient_id",
        "type": "transaction_type",
        "amount": "amount",
        "method": "payment_method",
        "fromdate": "transaction_start",
        "todate": "transaction_end",
        "placeofservice": "place_of_service",
        "procedurecode": "procedure_code",
        "modifier1": "modifier_1",
        "modifier2": "modifier_2",
        "diagnosisref1": "diagnosis_ref_1",
        "diagnosisref2": "diagnosis_ref_2",
        "diagnosisref3": "diagnosis_ref_3",
        "diagnosisref4": "diagnosis_ref_4",
        "units": "units",
        "departmentid": "department_id",
        "notes": "notes",
        "unitamount": "unit_amount",
        "transferoutid": "transfer_out_id",
        "transfertype": "transfer_type",
        "payments": "payment_amount",
        "adjustments": "adjustment_amount",
        "transfers": "transfer_amount",
        "outstanding": "outstanding_amount",
        "appointmentid": "appointment_id",
        "linenote": "line_note",
        "patientinsuranceid": "patient_insurance_id",
        "feescheduleid": "fee_schedule_id",
        "providerid": "provider_id",
        "supervisingproviderid": "supervising_provider_id",
    })

    # Convert dates
    df["transaction_start"] = pd.to_datetime(
        df["transaction_start"],
        errors="coerce",
        utc=True,
    )

    df["transaction_end"] = pd.to_datetime(
        df["transaction_end"],
        errors="coerce",
        utc=True,
    )

    # Add ingestion metadata
    ingested_at = pd.Timestamp.now(tz="UTC")

    df["_ingested_at"] = ingested_at
    df["_ingestion_date"] = ingested_at.date()
    df["_source_file"] = source_file

    return df
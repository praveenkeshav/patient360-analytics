from datetime import datetime, timedelta
from io import StringIO
import logging
import tempfile

import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from src.preprocessing.patient_preprocessing import preprocess_patients
from src.preprocessing.fhir_parser import (
    parse_fhir_bundle,
    classify_lab_abnormality,
)
from src.validation.lab_validation import validate_labs


# Airflow will show these messages in the task logs.
logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# S3 configuration
# ---------------------------------------------------------

BUCKET = "p360-healthcare-raw"

PATIENTS_KEY = "ehr/patients.csv"
FHIR_PREFIX = "fhir/"

LAB_REFERENCE_PATH = "data/raw/reference/lab_reference_ranges.csv"

LAB_OUTPUT_KEY = "processed/fhir/lab_observations.csv"


# ---------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------

# Retry tasks when a temporary failure occurs,
# such as an S3 or network problem.
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------

with DAG(
    dag_id="patient360_pipeline",
    start_date=datetime(2026, 8, 19),

    # Manual trigger during development.
    # Later we can change this to "@daily".
    schedule=None,

    catchup=False,
    default_args=default_args,
) as dag:

    # -----------------------------------------------------
    # Wait for source data
    # -----------------------------------------------------

    # Wait until patients.csv is available in S3.
    wait_for_patients = S3KeySensor(
        task_id="wait_for_patients",
        bucket_name=BUCKET,
        bucket_key=PATIENTS_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=300,
    )

    # Wait until FHIR JSON files are available in S3.
    wait_for_fhir = S3KeySensor(
        task_id="wait_for_fhir",
        bucket_name=BUCKET,
        bucket_key="fhir/*.json",
        wildcard_match=True,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=300,
    )


    # -----------------------------------------------------
    # Patient processing
    # -----------------------------------------------------

    @task
    def process_patients():

        # Connect to S3 using our Airflow AWS connection.
        s3 = S3Hook(aws_conn_id="aws_patient360")

        # Read patients.csv from S3.
        content = s3.read_key(
            key=PATIENTS_KEY,
            bucket_name=BUCKET,
        )

        # Convert the S3 file into a DataFrame.
        patients = pd.read_csv(
            StringIO(content)
        )

        # Apply our Python preprocessing logic.
        patients = preprocess_patients(patients)

        patient_count = len(patients)

        logger.info(
            "Patients processed: %s",
            patient_count,
        )

        # Return only a small value through XCom.
        # We do not pass the DataFrame through XCom.
        return patient_count


    # -----------------------------------------------------
    # FHIR laboratory processing
    # -----------------------------------------------------

    @task
    def process_fhir_labs():

        # Connect to S3.
        s3 = S3Hook(aws_conn_id="aws_patient360")

        # Load laboratory reference ranges.
        reference_df = pd.read_csv(
            LAB_REFERENCE_PATH
        )

        # Read patients.csv for patient ID validation.
        patient_content = s3.read_key(
            key=PATIENTS_KEY,
            bucket_name=BUCKET,
        )

        patients = pd.read_csv(
            StringIO(patient_content)
        )

        # Create a set of valid patient IDs.
        patient_ids = set(
            patients["Id"]
        )

        # Find all FHIR files in S3.
        keys = s3.list_keys(
            bucket_name=BUCKET,
            prefix=FHIR_PREFIX,
        )

        observations = []

        # Download each FHIR JSON file temporarily.
        with tempfile.TemporaryDirectory() as temp_dir:

            for key in keys:

                # Ignore folders and non-JSON objects.
                if not key.endswith(".json"):
                    continue

                file_path = s3.download_file(
                    key=key,
                    bucket_name=BUCKET,
                    local_path=temp_dir,
                )

                # Parse laboratory observations.
                result = parse_fhir_bundle(file_path)

                if not result.empty:
                    observations.append(result)

        # Make sure we found laboratory data.
        if not observations:
            raise ValueError(
                "No laboratory observations found."
            )

        # Combine observations from all FHIR files.
        labs = pd.concat(
            observations,
            ignore_index=True,
        )

        # Validate the laboratory data before transformation.
        if not validate_labs(
            labs,
            patient_ids,
        ):
            raise ValueError(
                "FHIR laboratory validation failed."
            )

        logger.info(
            "FHIR laboratory validation passed."
        )

        # Classify numeric laboratory results.
        labs = classify_lab_abnormality(
            labs,
            reference_df,
        )

        # Write the processed laboratory data to S3.
        s3.load_string(
            string_data=labs.to_csv(index=False),
            key=LAB_OUTPUT_KEY,
            bucket_name=BUCKET,
            replace=True,
        )

        logger.info(
            "FHIR files processed: %s",
            len(observations),
        )

        logger.info(
            "Laboratory observations: %s",
            len(labs),
        )

        logger.info(
            "Abnormal observations: %s",
            int(labs["is_abnormal"].sum()),
        )

        # Return only a small count through XCom.
        return len(labs)


    # -----------------------------------------------------
    # Pipeline summary
    # -----------------------------------------------------

    @task
    def report_counts(
        patient_count,
        lab_count,
    ):

        # These values are automatically passed through XCom.
        logger.info(
            "Patient 360 pipeline completed."
        )

        logger.info(
            "Patients processed: %s",
            patient_count,
        )

        logger.info(
            "Lab observations processed: %s",
            lab_count,
        )

        logger.info(
            "Processed output: s3://%s/%s",
            BUCKET,
            LAB_OUTPUT_KEY,
        )


    # -----------------------------------------------------
    # Task dependencies
    # -----------------------------------------------------

    patient_count = process_patients()

    lab_count = process_fhir_labs()

    # The summary runs after both processing tasks succeed.
    report_counts(
        patient_count,
        lab_count,
    )

    # Each sensor controls its corresponding processing task.
    wait_for_patients >> patient_count
    wait_for_fhir >> lab_count
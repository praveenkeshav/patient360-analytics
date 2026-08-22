from datetime import datetime, timedelta
from io import StringIO
import logging
import tempfile

import pandas as pd

from airflow import DAG
from airflow.sdk import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from src.preprocessing.patient_preprocessing import preprocess_patients
from src.preprocessing.encounter_preprocessing import preprocess_encounters
from src.preprocessing.condition_preprocessing import preprocess_conditions
from src.preprocessing.fhir_parser import (
    parse_fhir_bundle,
    classify_lab_abnormality,
)

from src.validation.patient_validation import validate_patients
from src.validation.encounter_validation import validate_encounters
from src.validation.condition_validation import validate_conditions
from src.validation.lab_validation import validate_labs


logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# S3 configuration
# ---------------------------------------------------------

BUCKET = "p360-healthcare-raw"

PATIENTS_KEY = "ehr/patients.csv"
ENCOUNTERS_KEY = "ehr/encounters.csv"
CONDITIONS_KEY = "ehr/conditions.csv"

PATIENTS_PROCESSED_KEY = "processed/ehr/patients.csv"
ENCOUNTERS_PROCESSED_KEY = "processed/ehr/encounters.parquet"
CONDITIONS_PROCESSED_KEY = "processed/ehr/conditions.csv"

FHIR_PREFIX = "fhir/"

LAB_REFERENCE_PATH = "data/raw/reference/lab_reference_ranges.csv"
LAB_OUTPUT_KEY = "processed/fhir/lab_observations.csv"


# ---------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------

with DAG(
    dag_id="ehr_ingestion",
    start_date=datetime(2026, 8, 19),
    schedule=None,
    catchup=False,
    default_args=default_args,
    template_searchpath="/usr/local/airflow/include",
) as dag:

    # -----------------------------------------------------
    # Wait for source data
    # -----------------------------------------------------

    wait_for_patients = S3KeySensor(
        task_id="wait_for_patients",
        bucket_name=BUCKET,
        bucket_key=PATIENTS_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=300,
    )

    wait_for_encounters = S3KeySensor(
        task_id="wait_for_encounters",
        bucket_name=BUCKET,
        bucket_key=ENCOUNTERS_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=300,
    )

    wait_for_conditions = S3KeySensor(
        task_id="wait_for_conditions",
        bucket_name=BUCKET,
        bucket_key=CONDITIONS_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=300,
    )

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

        s3 = S3Hook(aws_conn_id="aws_patient360")

        content = s3.read_key(
            key=PATIENTS_KEY,
            bucket_name=BUCKET,
        )

        patients = pd.read_csv(
            StringIO(content)
        )

        patients = preprocess_patients(patients)

        validate_patients(patients)

        s3.load_string(
            string_data=patients.to_csv(index=False),
            key=PATIENTS_PROCESSED_KEY,
            bucket_name=BUCKET,
            replace=True,
        )

        logger.info(
            "Patients processed: %s",
            len(patients),
        )

        return len(patients)

    # -----------------------------------------------------
    # Encounter processing
    # -----------------------------------------------------

    @task
    def process_encounters():

        s3 = S3Hook(aws_conn_id="aws_patient360")

        content = s3.read_key(
            key=ENCOUNTERS_KEY,
            bucket_name=BUCKET,
        )

        encounters = pd.read_csv(
            StringIO(content)
        )

        patient_content = s3.read_key(
            key=PATIENTS_KEY,
            bucket_name=BUCKET,
        )

        patients = pd.read_csv(
            StringIO(patient_content)
        )

        patient_ids = set(
            patients["Id"]
        )

        encounters = preprocess_encounters(
            encounters
        )

        validate_encounters(
            encounters,
            patient_ids,
        )

        with tempfile.NamedTemporaryFile(
            suffix=".parquet"
        ) as temp_file:

            encounters.to_parquet(
                temp_file.name,
                index=False,
            )

            s3.load_file(
                filename=temp_file.name,
                key=ENCOUNTERS_PROCESSED_KEY,
                bucket_name=BUCKET,
                replace=True,
            )

        logger.info(
            "Encounters processed: %s",
            len(encounters),
        )

        return len(encounters)

    # -----------------------------------------------------
    # Condition processing
    # -----------------------------------------------------

    @task
    def process_conditions():

        s3 = S3Hook(aws_conn_id="aws_patient360")

        content = s3.read_key(
            key=CONDITIONS_KEY,
            bucket_name=BUCKET,
        )

        conditions = pd.read_csv(
            StringIO(content)
        )

        patient_content = s3.read_key(
            key=PATIENTS_KEY,
            bucket_name=BUCKET,
        )

        patients = pd.read_csv(
            StringIO(patient_content)
        )

        patient_ids = set(
            patients["Id"]
        )

        encounter_content = s3.read_key(
            key=ENCOUNTERS_KEY,
            bucket_name=BUCKET,
        )

        encounters = pd.read_csv(
            StringIO(encounter_content)
        )

        encounter_ids = set(
            encounters["Id"]
        )

        conditions = preprocess_conditions(
            conditions
        )

        validate_conditions(
            conditions,
            patient_ids,
            encounter_ids,
        )

        s3.load_string(
            string_data=conditions.to_csv(index=False),
            key=CONDITIONS_PROCESSED_KEY,
            bucket_name=BUCKET,
            replace=True,
        )

        logger.info(
            "Conditions processed: %s",
            len(conditions),
        )

        return len(conditions)

    # -----------------------------------------------------
    # FHIR laboratory processing
    # -----------------------------------------------------

    @task
    def process_fhir_labs():

        s3 = S3Hook(aws_conn_id="aws_patient360")

        reference_df = pd.read_csv(
            LAB_REFERENCE_PATH
        )

        patient_content = s3.read_key(
            key=PATIENTS_KEY,
            bucket_name=BUCKET,
        )

        patients = pd.read_csv(
            StringIO(patient_content)
        )

        patient_ids = set(
            patients["Id"]
        )

        keys = s3.list_keys(
            bucket_name=BUCKET,
            prefix=FHIR_PREFIX,
        )

        observations = []

        with tempfile.TemporaryDirectory() as temp_dir:

            for key in keys:

                if not key.endswith(".json"):
                    continue

                file_path = s3.download_file(
                    key=key,
                    bucket_name=BUCKET,
                    local_path=temp_dir,
                )

                result = parse_fhir_bundle(file_path)

                if not result.empty:
                    observations.append(result)

        if not observations:
            raise ValueError(
                "No laboratory observations found."
            )

        labs = pd.concat(
            observations,
            ignore_index=True,
        )

        if not validate_labs(
            labs,
            patient_ids,
        ):
            raise ValueError(
                "FHIR laboratory validation failed."
            )

        labs = classify_lab_abnormality(
            labs,
            reference_df,
        )

        s3.load_string(
            string_data=labs.to_csv(index=False),
            key=LAB_OUTPUT_KEY,
            bucket_name=BUCKET,
            replace=True,
        )

        logger.info(
            "Laboratory observations processed: %s",
            len(labs),
        )

        return len(labs)

    # -----------------------------------------------------
    # Dependencies
    # -----------------------------------------------------

    patient_count = process_patients()

    encounter_count = process_encounters()

    condition_count = process_conditions()

    lab_count = process_fhir_labs()

    wait_for_patients >> patient_count

    wait_for_patients >> encounter_count
    wait_for_encounters >> encounter_count

    wait_for_patients >> condition_count
    wait_for_encounters >> condition_count
    wait_for_conditions >> condition_count

    wait_for_patients >> lab_count
    wait_for_fhir >> lab_count
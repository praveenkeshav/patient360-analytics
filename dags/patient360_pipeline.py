from datetime import datetime, timedelta
from io import StringIO
import logging
import tempfile

import pandas as pd

from airflow import DAG
from airflow.sdk import task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator

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


# Airflow will show these messages in the task logs.
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
    template_searchpath="/usr/local/airflow/include",
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

    # Wait until encounters.csv is available in S3.
    wait_for_encounters = S3KeySensor(
        task_id="wait_for_encounters",
        bucket_name=BUCKET,
        bucket_key=ENCOUNTERS_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=300,
    )

    # Wait until conditions.csv is available in S3.
    wait_for_conditions = S3KeySensor(
        task_id="wait_for_conditions",
        bucket_name=BUCKET,
        bucket_key=CONDITIONS_KEY,
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

        # Apply the reusable patient preprocessing function.
        # This standardizes column names, parses dates, and creates is_deceased.
        patients = preprocess_patients(patients)

        # Validate the processed patient data.
        # The validation checks patient IDs, birthdates, and date logic.
        validate_patients(patients)

        logger.info(
            "Patients validation passed."
        )


        # Write the processed DataFrame to CSV and save it to S3.
        # This creates the file that will later be loaded into Snowflake RAW.
        s3.load_string(
            string_data=patients.to_csv(index=False),
            key=PATIENTS_PROCESSED_KEY,
            bucket_name=BUCKET,
            replace=True,
        )

        # Get the number of processed patients for reporting.
        patient_count = len(patients)

        logger.info(
            "Patients processed: %s",
            patient_count,
        )

        # Log the S3 location of the processed patient file.
        logger.info(
            "Processed patient output: s3://%s/%s",
            BUCKET,
            PATIENTS_PROCESSED_KEY,
        )

        # Return only the row count through XCom.
        # We do not pass the DataFrame through XCom.
        return patient_count

    # -----------------------------------------------------
    # Encounter processing
    # -----------------------------------------------------

    @task
    def process_encounters():

        # Connect to S3 using our Airflow AWS connection.
        s3 = S3Hook(aws_conn_id="aws_patient360")

        # Read encounters.csv from S3.
        content = s3.read_key(
            key=ENCOUNTERS_KEY,
            bucket_name=BUCKET,
        )

        # Convert the S3 file into a DataFrame.
        encounters = pd.read_csv(
            StringIO(content)
        )

        # Read patients.csv to validate patient references.
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

        # Apply encounter preprocessing.
        # This standardizes columns, parses dates,
        # and calculates length_of_stay_days.
        encounters = preprocess_encounters(
            encounters
        )

        # Validate the processed encounter data.
        # This checks IDs, dates, and patient references.
        validate_encounters(
            encounters,
            patient_ids,
        )

        logger.info(
            "Encounter validation passed."
        )

        # Write the processed DataFrame as Parquet.
        # Parquet gives us hands-on practice with a columnar format.
        with tempfile.NamedTemporaryFile(
            suffix=".parquet"
        ) as temp_file:

            encounters.to_parquet(
                temp_file.name,
                index=False,
            )

            # Upload the Parquet file to S3.
            s3.load_file(
                filename=temp_file.name,
                key=ENCOUNTERS_PROCESSED_KEY,
                bucket_name=BUCKET,
                replace=True,
            )

        # Get the number of processed encounters.
        encounter_count = len(encounters)

        logger.info(
            "Encounters processed: %s",
            encounter_count,
        )

        # Log the processed S3 location.
        logger.info(
            "Processed encounter output: s3://%s/%s",
            BUCKET,
            ENCOUNTERS_PROCESSED_KEY,
        )

        # Return only the count through XCom.
        return encounter_count

    # -----------------------------------------------------
    # Condition processing
    # -----------------------------------------------------

    @task
    def process_conditions():

        # Connect to S3 using our Airflow AWS connection.
        s3 = S3Hook(aws_conn_id="aws_patient360")

        # Read conditions.csv from S3.
        content = s3.read_key(
            key=CONDITIONS_KEY,
            bucket_name=BUCKET,
        )

        # Convert the S3 file into a DataFrame.
        conditions = pd.read_csv(
            StringIO(content)
        )

        # Read patients.csv to validate patient references.
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

        # Read encounters.csv to validate encounter references.
        encounter_content = s3.read_key(
            key=ENCOUNTERS_KEY,
            bucket_name=BUCKET,
        )

        encounters = pd.read_csv(
            StringIO(encounter_content)
        )

        # Create a set of valid encounter IDs.
        encounter_ids = set(
            encounters["Id"]
        )

        # Apply the reusable Conditions preprocessing function.
        # This standardizes column names, parses dates,
        # and calculates condition duration.
        conditions = preprocess_conditions(
            conditions
        )

        # Validate the processed Conditions data.
        # This checks dates, patient references,
        # encounter references, and condition codes.
        validate_conditions(
            conditions,
            patient_ids,
            encounter_ids,
        )

        logger.info(
            "Condition validation passed."
        )

        # Write the processed Conditions DataFrame to CSV.
        # This file will later be loaded into Snowflake RAW.
        s3.load_string(
            string_data=conditions.to_csv(index=False),
            key=CONDITIONS_PROCESSED_KEY,
            bucket_name=BUCKET,
            replace=True,
        )

        # Get the number of processed conditions.
        condition_count = len(conditions)

        logger.info(
            "Conditions processed: %s",
            condition_count,
        )

        # Log the processed S3 location.
        logger.info(
            "Processed condition output: s3://%s/%s",
            BUCKET,
            CONDITIONS_PROCESSED_KEY,
        )

        # Return only the count through XCom.
        return condition_count

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
        encounter_count,
        condition_count,
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
        "Encounters processed: %s",
        encounter_count,

        )

        logger.info(
        "Conditions processed: %s",
        condition_count,

        )

        logger.info(
            "Lab observations processed: %s",
            lab_count,
        )

        logger.info(
        "Processed patient output: s3://%s/%s",
        BUCKET,
        PATIENTS_PROCESSED_KEY,

        )

        logger.info(
        "Processed encounter output: s3://%s/%s",
        BUCKET,
        ENCOUNTERS_PROCESSED_KEY,

        )

        logger.info(
        "Processed condition output: s3://%s/%s",
        BUCKET,
        CONDITIONS_PROCESSED_KEY,

        )

        logger.info(
            "Processed lab output: s3://%s/%s",
            BUCKET,
            LAB_OUTPUT_KEY,

        )

    # -----------------------------------------------------
    # Snowflake tasks
    # -----------------------------------------------------

    create_raw_load_audit = SQLExecuteQueryOperator(
        task_id="create_raw_load_audit",
        conn_id="snowflake_patient360",
        sql="sql/snowflake/create_raw_load_audit.sql",
    )
    
    @task_group(group_id="raw_patients_load")
    def raw_patients_load():

        create_raw_patients = SQLExecuteQueryOperator(
            task_id="create_raw_patients",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/create_raw_patients.sql",
        )

        load_raw_patients = SQLExecuteQueryOperator(
            task_id="load_raw_patients",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/load_raw_patients.sql",
        )

        validate_raw_patients = SQLCheckOperator(
            task_id="validate_raw_patients",
            conn_id="snowflake_patient360",
            sql="""
                SELECT
                    COUNT(*) >= 1000
                    AND COUNT(*) = COUNT(ID)
                    AND COUNT(*) = COUNT(DISTINCT ID)
                    AND COUNT_IF(BIRTHDATE > CURRENT_DATE) = 0
                FROM PATIENT360_PROD.EHR_RAW.RAW_PATIENTS
            """,
        )

        log_load_audit = SQLExecuteQueryOperator(
            task_id="log_load_audit",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/insert_raw_load_audit.sql",
            params={
                "table_name": "RAW_PATIENTS",
                "source_file": "patients.csv",
                "raw_table": "PATIENT360_PROD.EHR_RAW.RAW_PATIENTS",
            },
        )

        create_raw_patients >> load_raw_patients
        load_raw_patients >> validate_raw_patients
        validate_raw_patients >> log_load_audit

    @task_group(group_id="raw_encounters_load")
    def raw_encounters_load():

        create_raw_encounters = SQLExecuteQueryOperator(
            task_id="create_raw_encounters",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/create_raw_encounters.sql",
        )

        load_raw_encounters = SQLExecuteQueryOperator(
            task_id="load_raw_encounters",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/load_raw_encounters.sql",
        )

        validate_raw_encounters = SQLCheckOperator(
            task_id="validate_raw_encounters",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/validate_raw_encounters.sql",
        )

        log_load_audit = SQLExecuteQueryOperator(
            task_id="log_load_audit",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/insert_raw_load_audit.sql",
            params={
                "table_name": "RAW_ENCOUNTERS",
                "source_file": "encounters.parquet",
                "raw_table": "PATIENT360_PROD.EHR_RAW.RAW_ENCOUNTERS",
            },
        )

        create_raw_encounters >> load_raw_encounters
        load_raw_encounters >> validate_raw_encounters
        validate_raw_encounters >> log_load_audit

    @task_group(group_id="raw_conditions_load")
    def raw_conditions_load():

        create_raw_conditions = SQLExecuteQueryOperator(
            task_id="create_raw_conditions",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/create_raw_conditions.sql",
        )

        load_raw_conditions = SQLExecuteQueryOperator(
            task_id="load_raw_conditions",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/load_raw_conditions.sql",
        )

        validate_raw_conditions = SQLCheckOperator(
            task_id="validate_raw_conditions",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/validate_raw_conditions.sql",
        )

        log_load_audit = SQLExecuteQueryOperator(
            task_id="log_load_audit",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/insert_raw_load_audit.sql",
            params={
                "table_name": "RAW_CONDITIONS",
                "source_file": "conditions.csv",
                "raw_table": "PATIENT360_PROD.EHR_RAW.RAW_CONDITIONS",
            },
        )

        create_raw_conditions >> load_raw_conditions
        load_raw_conditions >> validate_raw_conditions
        validate_raw_conditions >> log_load_audit    


    @task_group(group_id="raw_labs_load")
    def raw_labs_load():

        create_raw_labs = SQLExecuteQueryOperator(
            task_id="create_raw_labs",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/create_raw_labs.sql",
        )

        load_raw_labs = SQLExecuteQueryOperator(
            task_id="load_raw_labs",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/load_raw_labs.sql",
        )

        validate_raw_labs = SQLCheckOperator(
            task_id="validate_raw_labs",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/validate_raw_labs.sql",
        )

        log_load_audit = SQLExecuteQueryOperator(
            task_id="log_load_audit",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/insert_raw_load_audit.sql",
            params={
                "table_name": "RAW_LABS",
                "source_file": "lab_observations.csv",
                "raw_table": "PATIENT360_PROD.EHR_RAW.RAW_LABS",
            },
        )

        create_raw_labs >> load_raw_labs
        load_raw_labs >> validate_raw_labs
        validate_raw_labs >> log_load_audit

    # -----------------------------------------------------
    # Task dependencies
    # -----------------------------------------------------

    patient_count = process_patients()
    encounter_count = process_encounters()
    condition_count = process_conditions()
    lab_count = process_fhir_labs()

    # ------------------------------------------------------
    # Source data dependencies
    # ------------------------------------------------------

    # Patients processing requires patients source data.
    wait_for_patients >> patient_count

    # Encounters processing requires both patients and encounters.
    wait_for_patients >> encounter_count
    wait_for_encounters >> encounter_count

    # Conditions processing requires patients and encounters
    wait_for_patients >> condition_count
    wait_for_encounters >> condition_count
    wait_for_conditions >> condition_count

    # FHIR Labs processing requires patients and FHIR data.
    wait_for_patients >> lab_count
    wait_for_fhir >> lab_count

    # -----------------------------------------------------
    # Create RAW audit table before any RAW load starts.
    # -----------------------------------------------------

    raw_patients_load_group = raw_patients_load()
    raw_encounters_load_group = raw_encounters_load()
    raw_conditions_load_group = raw_conditions_load()
    raw_labs_load_group = raw_labs_load()

    create_raw_load_audit >> raw_patients_load_group
    create_raw_load_audit >> raw_encounters_load_group
    create_raw_load_audit >> raw_conditions_load_group
    create_raw_load_audit >> raw_labs_load_group

    # -----------------------------------------------------
    # Processing must complete before corresponding RAW load
    # -----------------------------------------------------

    patient_count >> raw_patients_load_group
    encounter_count >> raw_encounters_load_group
    condition_count >> raw_conditions_load_group
    lab_count >> raw_labs_load_group

    # -----------------------------------------------------
    # Final pipeline summary.
    # -----------------------------------------------------

    summary = report_counts(
        patient_count,
        encounter_count,
        condition_count,
        lab_count,
    )

    # Report completion only after all RAW load groups succeed.
    [
        raw_patients_load_group,
        raw_encounters_load_group,
        raw_conditions_load_group,
        raw_labs_load_group,
    ] >> summary
    
    
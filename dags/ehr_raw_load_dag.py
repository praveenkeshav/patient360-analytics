from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
    SQLCheckOperator,
)

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

BUCKET = "p360-healthcare-raw"

PATIENTS_PROCESSED_KEY = "processed/ehr/patients.csv"
ENCOUNTERS_PROCESSED_KEY = "processed/ehr/encounters.parquet"
CONDITIONS_PROCESSED_KEY = "processed/ehr/conditions.csv"
LABS_PROCESSED_KEY = "processed/fhir/lab_observations.csv"


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}

# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------

with DAG(
    dag_id="ehr_raw_load",
    start_date=datetime(2026, 8, 19),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    template_searchpath="/usr/local/airflow/include",
    tags=["patient360", "ehr", "snowflake", "raw"],
    doc_md="""
    ### Patient 360 EHR RAW Load

    Loads validated EHR/FHIR files from processed S3 into the Snowflake RAW layer.
    The DAG is scheduled after the daily EHR ingestion window and validates each
    RAW table before writing its audit record.
    """,
) as dag:

    # -----------------------------------------------------
    # Wait for processed files in S3
    # -----------------------------------------------------

    wait_for_patients = S3KeySensor(
        task_id="wait_for_processed_patients",
        bucket_name=BUCKET,
        bucket_key=PATIENTS_PROCESSED_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=timedelta(minutes=30),
        deferrable=True,
    )

    wait_for_encounters = S3KeySensor(
        task_id="wait_for_processed_encounters",
        bucket_name=BUCKET,
        bucket_key=ENCOUNTERS_PROCESSED_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=timedelta(minutes=30),
        deferrable=True,
    )

    wait_for_conditions = S3KeySensor(
        task_id="wait_for_processed_conditions",
        bucket_name=BUCKET,
        bucket_key=CONDITIONS_PROCESSED_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=timedelta(minutes=30),
        deferrable=True,
    )

    wait_for_labs = S3KeySensor(
        task_id="wait_for_processed_labs",
        bucket_name=BUCKET,
        bucket_key=LABS_PROCESSED_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=timedelta(minutes=30),
        deferrable=True,
    )

    # -----------------------------------------------------
    # Create audit table
    # -----------------------------------------------------

    create_raw_load_audit = SQLExecuteQueryOperator(
        task_id="create_raw_load_audit",
        conn_id="snowflake_patient360",
        sql="sql/snowflake/create_raw_load_audit.sql",
    )

    # =====================================================
    # PATIENTS
    # =====================================================

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

    audit_raw_patients = SQLExecuteQueryOperator(
        task_id="audit_raw_patients",
        conn_id="snowflake_patient360",
        sql="sql/snowflake/insert_raw_load_audit.sql",
        params={
            "table_name": "RAW_PATIENTS",
            "source_file": "patients.csv",
            "raw_table": "PATIENT360_PROD.EHR_RAW.RAW_PATIENTS",
        },
    )

    # =====================================================
    # ENCOUNTERS
    # =====================================================

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

    audit_raw_encounters = SQLExecuteQueryOperator(
        task_id="audit_raw_encounters",
        conn_id="snowflake_patient360",
        sql="sql/snowflake/insert_raw_load_audit.sql",
        params={
            "table_name": "RAW_ENCOUNTERS",
            "source_file": "encounters.parquet",
            "raw_table": "PATIENT360_PROD.EHR_RAW.RAW_ENCOUNTERS",
        },
    )

    # =====================================================
    # CONDITIONS
    # =====================================================

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

    audit_raw_conditions = SQLExecuteQueryOperator(
        task_id="audit_raw_conditions",
        conn_id="snowflake_patient360",
        sql="sql/snowflake/insert_raw_load_audit.sql",
        params={
            "table_name": "RAW_CONDITIONS",
            "source_file": "conditions.csv",
            "raw_table": "PATIENT360_PROD.EHR_RAW.RAW_CONDITIONS",
        },
    )

    # =====================================================
    # LABS
    # =====================================================

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

    audit_raw_labs = SQLExecuteQueryOperator(
        task_id="audit_raw_labs",
        conn_id="snowflake_patient360",
        sql="sql/snowflake/insert_raw_load_audit.sql",
        params={
            "table_name": "RAW_LABS",
            "source_file": "lab_observations.csv",
            "raw_table": "PATIENT360_PROD.EHR_RAW.RAW_LABS",
        },
    )

    # =====================================================
    # DEPENDENCIES
    # =====================================================

    # Audit table must exist before audit records are written.
    create_raw_load_audit >> [
        audit_raw_patients,
        audit_raw_encounters,
        audit_raw_conditions,
        audit_raw_labs,
    ]

    # Patients
    wait_for_patients >> create_raw_patients
    create_raw_patients >> load_raw_patients
    load_raw_patients >> validate_raw_patients
    validate_raw_patients >> audit_raw_patients

    # Encounters
    wait_for_encounters >> create_raw_encounters
    create_raw_encounters >> load_raw_encounters
    load_raw_encounters >> validate_raw_encounters
    validate_raw_encounters >> audit_raw_encounters

    # Conditions
    wait_for_conditions >> create_raw_conditions
    create_raw_conditions >> load_raw_conditions
    load_raw_conditions >> validate_raw_conditions
    validate_raw_conditions >> audit_raw_conditions

    # Labs
    wait_for_labs >> create_raw_labs
    create_raw_labs >> load_raw_labs
    load_raw_labs >> validate_raw_labs
    validate_raw_labs >> audit_raw_labs
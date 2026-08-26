from datetime import datetime, timedelta
from io import StringIO
import logging
import os
import tempfile

import pandas as pd

from airflow import DAG
from airflow.sdk import task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator,
    SQLCheckOperator,
)

from src.preprocessing.claims_preprocessing import preprocess_claims
from src.preprocessing.claim_transactions_preprocessing import (
    preprocess_claim_transactions,
)

from src.validation.claims_validation import validate_claims
from src.validation.claim_transactions_validation import (
    validate_claim_transactions,
)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# S3 configuration
# ---------------------------------------------------------

BUCKET = "p360-healthcare-raw"

CLAIMS_KEY = "claims/claims.csv"
CLAIM_TRANSACTIONS_KEY = "claims/claims_transactions.csv"

CLAIMS_PROCESSED_KEY = "processed/claims/claims.csv"
CLAIM_TRANSACTIONS_PROCESSED_KEY = (
    "processed/claims/claims_transactions.csv"
)


# ---------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------

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
    dag_id="claims_ingestion",
    start_date=datetime(2026, 8, 22),
    schedule="30 1 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=90),
    default_args=default_args,
    tags=["patient360", "claims", "production", "ingestion"],
    doc_md="""
    ## Patient 360 Claims Ingestion

    Daily production ingestion for claims and claim transactions.
    The DAG waits for both source files, validates/preprocesses them,
    and loads the validated outputs into Snowflake RAW tables.
    """,
    template_searchpath="/usr/local/airflow/include",

) as dag:

    # =====================================================
    # Wait for source data
    # =====================================================

    wait_for_claims = S3KeySensor(
        task_id="wait_for_claims",
        bucket_name=BUCKET,
        bucket_key=CLAIMS_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=1800,
        deferrable=True,
    )

    wait_for_claim_transactions = S3KeySensor(
        task_id="wait_for_claim_transactions",
        bucket_name=BUCKET,
        bucket_key=CLAIM_TRANSACTIONS_KEY,
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=1800,
        deferrable=True,
    )

    # =====================================================
    # Claims preprocessing + validation
    # =====================================================

    @task
    def process_claims():

        s3 = S3Hook(
            aws_conn_id="aws_patient360"
        )

        # Read source file from S3.
        content = s3.read_key(
            key=CLAIMS_KEY,
            bucket_name=BUCKET,
        )

        claims = pd.read_csv(
            StringIO(content)
        )

        # Preprocess.
        claims = preprocess_claims(
            claims
        )

        # Validate.
        validate_claims(
            claims
        )

        logger.info(
            "Claims validation passed."
        )

        # Write processed file to S3.
        s3.load_string(
            string_data=claims.to_csv(index=False),
            key=CLAIMS_PROCESSED_KEY,
            bucket_name=BUCKET,
            replace=True,
        )

        logger.info(
            "Claims processed: %s",
            len(claims),
        )

        logger.info(
            "Processed claims output: s3://%s/%s",
            BUCKET,
            CLAIMS_PROCESSED_KEY,
        )

        return len(claims)

    # =====================================================
    # Claim transaction preprocessing + validation
    # =====================================================

    @task
    def process_claim_transactions():

        s3 = S3Hook(
            aws_conn_id="aws_patient360"
        )

        # Use an isolated temporary directory so retries/runs cannot
        # collide with another worker process.
        with tempfile.TemporaryDirectory(
            prefix="patient360_claims_"
        ) as temp_dir:

            # Download source file from S3 to local worker storage.
            input_file = s3.download_file(
                key=CLAIM_TRANSACTIONS_KEY,
                bucket_name=BUCKET,
                local_path=temp_dir,
            )

            output_file = os.path.join(
                temp_dir,
                "claims_transactions_processed.csv",
            )

            logger.info(
                "Downloaded claim transactions file from s3://%s/%s",
                BUCKET,
                CLAIM_TRANSACTIONS_KEY,
            )

            seen_transaction_ids = set()
            first_chunk = True
            total_rows = 0

            # Process the large CSV in chunks.
            for transactions in pd.read_csv(
                input_file,
                chunksize=50000,
            ):

                # Preprocess current chunk.
                transactions = preprocess_claim_transactions(
                    transactions
                )

                # Check duplicate transaction IDs across chunks.
                transaction_ids = set(
                    transactions["transaction_id"].dropna()
                )

                if seen_transaction_ids.intersection(transaction_ids):
                    raise ValueError(
                        "Duplicate transaction_id found across chunks."
                    )

                seen_transaction_ids.update(transaction_ids)

                # Validate current chunk.
                if not validate_claim_transactions(
                    transactions
                ):
                    raise ValueError(
                        "Claim transaction validation failed."
                    )

                # Write validated chunk.
                transactions.to_csv(
                    output_file,
                    mode="w" if first_chunk else "a",
                    header=first_chunk,
                    index=False,
                )

                first_chunk = False
                total_rows += len(transactions)

                logger.info(
                    "Processed %s claim transactions so far.",
                    total_rows,
                )

            if first_chunk:
                raise ValueError(
                    "Claim transactions source file is empty."
                )

            # Upload only after every chunk has passed validation.
            s3.load_file(
                filename=output_file,
                key=CLAIM_TRANSACTIONS_PROCESSED_KEY,
                bucket_name=BUCKET,
                replace=True,
            )

            logger.info(
                "Claim transactions processed: %s",
                total_rows,
            )

            logger.info(
                "Processed claim transactions output: s3://%s/%s",
                BUCKET,
                CLAIM_TRANSACTIONS_PROCESSED_KEY,
            )

            return total_rows

    # =====================================================
    # Create RAW load audit table
    # =====================================================

    create_raw_load_audit = SQLExecuteQueryOperator(
        task_id="create_raw_load_audit",
        conn_id="snowflake_patient360",
        sql="sql/snowflake/create_raw_load_audit.sql",
    )

    # =====================================================
    # RAW CLAIMS load
    # =====================================================

    @task_group(group_id="raw_claims_load")
    def raw_claims_load():

        create_raw_claims = SQLExecuteQueryOperator(
            task_id="create_raw_claims",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/create_raw_claims.sql",
        )

        load_raw_claims = SQLExecuteQueryOperator(
            task_id="load_raw_claims",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/load_raw_claims.sql",
        )

        validate_raw_claims = SQLCheckOperator(
            task_id="validate_raw_claims",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/validate_raw_claims.sql",
        )

        audit_raw_claims = SQLExecuteQueryOperator(
            task_id="audit_raw_claims",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/insert_raw_load_audit.sql",
            params={
                "table_name": "RAW_CLAIMS",
                "source_file": "claims.csv",
                "raw_table": "PATIENT360_PROD.CLAIMS_RAW.RAW_CLAIMS",
            },
        )

        create_raw_claims >> load_raw_claims
        load_raw_claims >> validate_raw_claims
        validate_raw_claims >> audit_raw_claims

    # =====================================================
    # RAW CLAIM TRANSACTIONS load
    # =====================================================

    @task_group(group_id="raw_claim_transactions_load")
    def raw_claim_transactions_load():

        create_raw_claim_transactions = SQLExecuteQueryOperator(
            task_id="create_raw_claim_transactions",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/create_raw_claim_transactions.sql",
        )

        load_raw_claim_transactions = SQLExecuteQueryOperator(
            task_id="load_raw_claim_transactions",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/load_raw_claim_transactions.sql",
        )

        validate_raw_claim_transactions = SQLCheckOperator(
            task_id="validate_raw_claim_transactions",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/validate_raw_claim_transactions.sql",
        )

        audit_raw_claim_transactions = SQLExecuteQueryOperator(
            task_id="audit_raw_claim_transactions",
            conn_id="snowflake_patient360",
            sql="sql/snowflake/insert_raw_load_audit.sql",
            params={
                "table_name": "RAW_CLAIM_TRANSACTIONS",
                "source_file": "claims_transactions.csv",
                "raw_table": "PATIENT360_PROD.CLAIMS_RAW.RAW_CLAIM_TRANSACTIONS",
            },
        )

        (
            create_raw_claim_transactions
            >> load_raw_claim_transactions
            >> validate_raw_claim_transactions
            >> audit_raw_claim_transactions
        )

    # =====================================================
    # Task dependencies
    # =====================================================

    claim_count = process_claims()

    transaction_count = process_claim_transactions()

    # Source → preprocessing
    wait_for_claims >> claim_count
    wait_for_claim_transactions >> transaction_count

    # Create audit table first.
    raw_claims_load_group = raw_claims_load()
    raw_claim_transactions_load_group = raw_claim_transactions_load()

    create_raw_load_audit >> raw_claims_load_group
    create_raw_load_audit >> raw_claim_transactions_load_group

    # Processing → RAW loading
    claim_count >> raw_claims_load_group
    transaction_count >> raw_claim_transactions_load_group
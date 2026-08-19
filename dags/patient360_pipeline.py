from datetime import datetime
from io import StringIO
import tempfile

from src.preprocessing.fhir_parser import (
    parse_fhir_bundle,
    classify_lab_abnormality,
)

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from src.preprocessing.patients import preprocess_patients

BUCKET = "p360-healthcare-raw"
KEY = "ehr/patients.csv"

with DAG(
    dag_id="patient360_pipeline",
    start_date=datetime(2026, 8, 19),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_file = S3KeySensor(
        task_id="wait_for_patients_file",
        bucket_name=BUCKET,
        bucket_key=KEY,
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
    

    @task
    def process_patients():
        s3 = S3Hook(aws_conn_id="aws_patient360")

        file_content = s3.read_key(KEY, BUCKET)
        df = pd.read_csv(StringIO(file_content))

        df = preprocess_patients(df)

        print(f"Patients processed: {len(df)}")

    @task
    def process_fhir():
        s3 = S3Hook(aws_conn_id="aws_patient360")

        reference_path = "data/raw/reference/lab_reference_ranges.csv"
        reference_df = pd.read_csv(reference_path)

        keys = s3.list_keys(bucket_name=BUCKET, prefix="fhir/")

        all_observations = []

        with tempfile.TemporaryDirectory() as temp_dir:
            for key in keys:
                if not key.endswith(".json"):
                    continue

                file_path = s3.download_file(
                    key=key,
                    bucket_name=BUCKET,
                    local_path=temp_dir,
                )

                observations = parse_fhir_bundle(file_path)
                all_observations.append(observations)

        observations_df = pd.concat(all_observations, ignore_index=True)

        result = classify_lab_abnormality(
            observations_df,
            reference_df,
        )

        print(f"FHIR files processed: {len(all_observations)}")
        print(f"Lab observations: {len(result)}")
        print(f"Abnormal observations: {result['is_abnormal'].sum()}")
       

    wait_for_file >> process_patients()
    wait_for_fhir >> process_fhir()
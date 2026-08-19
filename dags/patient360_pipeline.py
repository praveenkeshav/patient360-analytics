from datetime import datetime
from io import StringIO

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

    @task
    def process_patients():
        s3 = S3Hook(aws_conn_id="aws_patient360")

        file_content = s3.read_key(KEY, BUCKET)
        df = pd.read_csv(StringIO(file_content))

        df = preprocess_patients(df)

        print(f"Patients processed: {len(df)}")

    wait_for_file >> process_patients()
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id = "patient360_pipeline",
    start_date =datetime(2026, 8, 19),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_file = S3KeySensor(
        task_id ="wait_for_patients_file",
        bucket_name="p360-healthcare-raw",
        bucket_key="ehr/patients.csv",
        aws_conn_id="aws_patient360",
        poke_interval=30,
        timeout=300
    )

    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )

    wait_for_file >> start_pipeline
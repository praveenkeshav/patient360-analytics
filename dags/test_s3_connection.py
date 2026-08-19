from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime


@dag(
    schedule=None,
    start_date=datetime(2026, 8, 18),
    catchup=False,
    tags=["patient360", "s3"],
)
def test_s3_connection():

    @task
    def test_s3():
        hook = S3Hook(aws_conn_id="aws_patient360")

        keys = hook.list_keys(
            bucket_name="p360-healthcare-raw",
            prefix="ehr/"
        )

        print("S3 files found:")
        for key in keys:
            print(key)

        if not keys:
            raise ValueError("No files found in S3 bucket.")

    test_s3()


test_s3_connection()
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


# ---------------------------------------------------------
# dbt configuration
# ---------------------------------------------------------

DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt"


profile_config = ProfileConfig(
    profile_name="patient360",
    target_name="prod",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_patient360",
    ),
)

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}

# ---------------------------------------------------------
# Daily production dbt build
# ---------------------------------------------------------
# No full_refresh flag is supplied here. Once the selected dbt
# models are materialized incrementally, dbt will use their
# incremental strategy on normal production runs.

with DAG(
    dag_id="patient360_dbt_models",
    start_date=datetime(2026, 8, 19),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    tags=["patient360", "dbt", "production", "daily"],
    description="Daily production dbt build for Patient 360 marts.",
) as dag:

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",

        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            install_dbt_deps=False,
        ),

        profile_config=profile_config,
        render_config=RenderConfig(
            select=[
                "path:models/staging",
                "path:models/intermediate",
                "path:models/marts",
            ],
            test_behavior=TestBehavior.AFTER_EACH,

        ),

        operator_args={
            "cancel_query_on_kill": True,
        },
    )
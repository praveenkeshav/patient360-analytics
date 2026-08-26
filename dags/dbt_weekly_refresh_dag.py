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
# Weekly production refresh
# ---------------------------------------------------------
# This is intentionally separate from the daily DAG so that the
# daily production build remains incremental while the weekly
# maintenance run can explicitly rebuild incremental models.

with DAG(
    dag_id="patient360_dbt_weekly_refresh",
    start_date=datetime(2026, 8, 19),
    schedule="0 4 * * 0",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    default_args=default_args,
    tags=["patient360", "dbt", "production", "weekly", "refresh"],
    description="Weekly full refresh of Patient 360 dbt production models.",
) as dag:

    dbt_weekly_refresh = DbtTaskGroup(
        group_id="dbt_weekly_refresh",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
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
            "install_deps": True,
            "full_refresh": True,
            "cancel_query_on_kill": True,
        },
    )

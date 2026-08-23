from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    RenderConfig,
)
from cosmos.constants import TestBehavior
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


# ---------------------------------------------------------
# dbt configuration
# ---------------------------------------------------------

DBT_PROJECT_PATH = Path(__file__).parent.parent / "dbt"


profile_config = ProfileConfig(
    profile_name="patient360",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_patient360",
    ),
)


# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------

with DAG(
    dag_id="patient360_dbt_models",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["patient360", "dbt"],
) as dag:

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",

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
        },
    )
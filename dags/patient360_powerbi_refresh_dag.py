from datetime import datetime

import requests

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook


# -------------------------------------------------------------------
# Power BI configuration
# -------------------------------------------------------------------

POWERBI_CONNECTION_ID = "powerbi_patient360"
POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"

WORKSPACE_NAME = "Patient360"
SEMANTIC_MODEL_NAME = "p360_dashboard"


# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------

with DAG(
    dag_id="patient360_powerbi_refresh",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["patient360", "powerbi"],
) as dag:

    @task
    def refresh_semantic_model():

        # -----------------------------------------------------------
        # Get Power BI service-principal credentials from Airflow
        # -----------------------------------------------------------

        conn = BaseHook.get_connection(POWERBI_CONNECTION_ID)

        client_id = conn.login
        client_secret = conn.password
        tenant_id = conn.extra_dejson.get("tenant_id")

        if not client_id or not client_secret or not tenant_id:
            raise ValueError(
                "Power BI connection is missing client ID, "
                "client secret, or tenant ID."
            )

        # -----------------------------------------------------------
        # Authenticate with Microsoft Entra ID
        # -----------------------------------------------------------

        token_url = (
            f"https://login.microsoftonline.com/"
            f"{tenant_id}/oauth2/v2.0/token"
        )

        token_response = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "https://analysis.windows.net/powerbi/api/.default",
            },
            timeout=30,
        )

        if token_response.status_code != 200:
            raise RuntimeError(
                f"Power BI authentication failed: "
                f"{token_response.status_code} - "
                f"{token_response.text}"
            )

        access_token = token_response.json()["access_token"]

        headers = {
            "Authorization": f"Bearer {access_token}",
        }

        # -----------------------------------------------------------
        # Find Patient360 workspace
        # -----------------------------------------------------------

        workspaces_response = requests.get(
            f"{POWERBI_API_BASE}/groups",
            headers=headers,
            timeout=30,
        )

        if workspaces_response.status_code != 200:
            raise RuntimeError(
                f"Unable to retrieve Power BI workspaces: "
                f"{workspaces_response.status_code} - "
                f"{workspaces_response.text}"
            )

        workspaces = workspaces_response.json().get("value", [])

        workspace = next(
            (
                w
                for w in workspaces
                if w.get("name") == WORKSPACE_NAME
            ),
            None,
        )

        if not workspace:
            raise RuntimeError(
                f"Power BI workspace '{WORKSPACE_NAME}' was not found."
            )

        workspace_id = workspace["id"]

        # -----------------------------------------------------------
        # Find semantic model
        # -----------------------------------------------------------

        datasets_response = requests.get(
            f"{POWERBI_API_BASE}/groups/{workspace_id}/datasets",
            headers=headers,
            timeout=30,
        )

        if datasets_response.status_code != 200:
            raise RuntimeError(
                f"Unable to retrieve semantic models: "
                f"{datasets_response.status_code} - "
                f"{datasets_response.text}"
            )

        datasets = datasets_response.json().get("value", [])

        dataset = next(
            (
                d
                for d in datasets
                if d.get("name") == SEMANTIC_MODEL_NAME
            ),
            None,
        )

        if not dataset:
            raise RuntimeError(
                f"Semantic model '{SEMANTIC_MODEL_NAME}' "
                f"was not found in workspace '{WORKSPACE_NAME}'."
            )

        dataset_id = dataset["id"]

        # -----------------------------------------------------------
        # Trigger refresh
        # -----------------------------------------------------------

        refresh_url = (
            f"{POWERBI_API_BASE}/groups/"
            f"{workspace_id}/datasets/{dataset_id}/refreshes"
        )

        refresh_response = requests.post(
            refresh_url,
            headers=headers,
            json={},
            timeout=30,
        )

        if refresh_response.status_code not in (200, 202):
            raise RuntimeError(
                f"Power BI refresh failed: "
                f"{refresh_response.status_code} - "
                f"{refresh_response.text}"
            )

        print(
            f"Power BI refresh request accepted for "
            f"'{SEMANTIC_MODEL_NAME}'."
        )


    refresh_semantic_model()

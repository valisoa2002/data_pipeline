"""
dag_dbt_transform.py
PROJET VALISOA 2026 — Couche Gold (Silver → Gold via dbt)

Orchestration dbt (projet valisoa_gold) via Astronomer Cosmos : chaque
modèle dbt (staging/intermediate/marts) devient une task Airflow, avec
le lignage dbt préservé dans le graphe Airflow.

Deux modes d'exécution possibles (au choix, via Param `refresh_mode`) :
  - incremental : rejoue uniquement fact_* en incrémental (rapide,
                  destiné à être déclenché juste après dag_excel_quality)
  - full_refresh : recalcule tout, y compris les dimensions et
                   dim_temps (planifié, ex. nocturne)

Ce DAG est schedulé quotidiennement (full refresh de sécurité) ET peut
être déclenché manuellement/par TriggerDagRunOperator depuis
dag_excel_quality pour un refresh incrémental après chaque ingestion.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode
from airflow.sdk import Variable

DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/gold_layer"
DBT_PROFILES_DIR = DBT_PROJECT_DIR  # profiles.yml colocalisé avec le projet

# Les credentials Postgres sont injectés en variables d'env, lues par
# profiles.yml (env_var(...)), cohérent avec pg_staging_conn utilisé
# par les DAGs 1 et 2.
PG_CONN_STR = Variable.get(
    "pg_staging_conn",
    default="postgresql://airflow:airflow@postgres:5432/airflow",
)

profile_config = ProfileConfig(
    profile_name="valisoa_gold",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROFILES_DIR}/profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
)

execution_config = ExecutionConfig(
    execution_mode="local",  # dbt exécuté dans le même venv qu'Airflow
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
    # Sélection par défaut : tout le projet. Le mode incrémental/full
    # est géré nativement par la config `is_incremental()` dans les
    # modèles fact_* ; pas besoin de --full-refresh sauf reset complet.
)

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG planifié — filet de sécurité quotidien (recalcul complet des
# dimensions + facts en incrémental standard)
# ---------------------------------------------------------------------------
dag_dbt_transform = DbtDag(
    dag_id="dag_dbt_transform",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    schedule="0 3 * * *",   # tous les jours à 3h — recalcul de sécurité
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["valisoa", "gold", "dbt", "dwh"],
    env_vars={
        "DBT_PG_HOST": os.getenv("DBT_PG_HOST", "postgres"),
        "DBT_PG_USER": os.getenv("DBT_PG_USER", "airflow"),
        "DBT_PG_PASSWORD": os.getenv("DBT_PG_PASSWORD", "airflow"),
        "DBT_PG_PORT": os.getenv("DBT_PG_PORT", "5432"),
        "DBT_PG_DBNAME": os.getenv("DBT_PG_DBNAME", "airflow"),
    },
)
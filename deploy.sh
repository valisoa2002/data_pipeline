#!/bin/bash
# =============================================================================
# deploy.sh — Déploiement dag_excel_ingestion sur PROJET VALISOA 2026
# À exécuter depuis le dossier racine de ton projet (là où est docker-compose.yml)
# =============================================================================

set -e  # Arrêt immédiat si une commande échoue

AIRFLOW_CONTAINER="data_pipeline-airflow-1"
POSTGRES_CONTAINER="data_pipeline-postgres-1"
DAG_FILE="dag_excel_ingestion.py"
DAGS_LOCAL_FOLDER="./airflow/dags"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║   VALISOA 2026 — Déploiement dag_excel_ingestion    ║"
echo "╚══════════════════════════════════════════════════════╝"

# -----------------------------------------------------------------------------
# ÉTAPE 1 — Copie du DAG dans le dossier mappé
# -----------------------------------------------------------------------------
echo ""
echo "► [1/5] Copie du DAG..."
cp "$DAG_FILE" "$DAGS_LOCAL_FOLDER/$DAG_FILE"
echo "    ✓ $DAGS_LOCAL_FOLDER/$DAG_FILE"

# -----------------------------------------------------------------------------
# ÉTAPE 2 — Dossier de dépôt Excel dans le container Airflow
# -----------------------------------------------------------------------------
echo ""
echo "► [2/5] Création du dossier de dépôt Excel..."
docker exec "$AIRFLOW_CONTAINER" mkdir -p /opt/airflow/data/excel_drop
docker exec "$AIRFLOW_CONTAINER" mkdir -p /opt/airflow/data/excel_drop/archive
echo "    ✓ /opt/airflow/data/excel_drop"

# -----------------------------------------------------------------------------
# ÉTAPE 3 — Installation des dépendances Python dans le container Airflow
# -----------------------------------------------------------------------------
echo ""
echo "► [3/5] Installation des dépendances Python (openpyxl, sqlalchemy, pandas)..."
docker exec "$AIRFLOW_CONTAINER" pip install --quiet \
    openpyxl==3.1.5 \
    pandas==2.2.3 \
    sqlalchemy==2.0.36 \
    psycopg2-binary==2.9.10
echo "    ✓ Dépendances installées"

# -----------------------------------------------------------------------------
# ÉTAPE 4 — Variables Airflow
# -----------------------------------------------------------------------------
echo ""
echo "► [4/5] Création des Airflow Variables..."

docker exec "$AIRFLOW_CONTAINER" airflow variables set \
    excel_drop_folder "/opt/airflow/data/excel_drop"
echo "    ✓ excel_drop_folder = /opt/airflow/data/excel_drop"

docker exec "$AIRFLOW_CONTAINER" airflow variables set \
    pg_staging_conn "postgresql://airflow:airflow@postgres:5432/airflow"
echo "    ✓ pg_staging_conn = postgresql://airflow:airflow@postgres:5432/airflow"

# -----------------------------------------------------------------------------
# ÉTAPE 5 — Schéma staging dans PostgreSQL
# -----------------------------------------------------------------------------
echo ""
echo "► [5/5] Création du schéma 'staging' dans PostgreSQL..."
docker exec "$POSTGRES_CONTAINER" psql -U airflow -d airflow -c \
    "CREATE SCHEMA IF NOT EXISTS staging;"
echo "    ✓ Schéma staging prêt"

# -----------------------------------------------------------------------------
# RÉSUMÉ
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║                   ✓ DÉPLOIEMENT OK                  ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "  Prochaines étapes :"
echo ""
echo "  1. Déposer un fichier Excel :"
echo "     docker cp Rapport_Production_*.xlsx \\"
echo "       $AIRFLOW_CONTAINER:/opt/airflow/data/excel_drop/"
echo ""
echo "  2. Déclencher le DAG manuellement :"
echo "     docker exec $AIRFLOW_CONTAINER \\"
echo "       airflow dags trigger dag_excel_ingestion"
echo ""
echo "  3. Suivre l'exécution :"
echo "     → http://localhost:8080  (DAGs → dag_excel_ingestion)"
echo ""
echo "  4. Vérifier les données en base :"
echo "     docker exec $POSTGRES_CONTAINER \\"
echo "       psql -U airflow -d airflow -c \\"
echo "       'SELECT COUNT(*) FROM staging.raw_excel_realisations;'"
echo ""
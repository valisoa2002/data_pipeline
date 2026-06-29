#!/bin/bash
# Fix installation des dépendances Python dans le container Airflow
# Le user airflow n'a pas les droits sur /root/bin/pip → on utilise python -m pip

AIRFLOW_CONTAINER="data_pipeline-airflow-1"

echo ""
echo "► Installation des dépendances via python -m pip..."

docker exec --user root "$AIRFLOW_CONTAINER" python -m pip install --quiet \
    openpyxl==3.1.5 \
    pandas==2.2.3 \
    sqlalchemy==2.0.36 \
    psycopg2-binary==2.9.10

echo "    ✓ Dépendances installées"

echo ""
echo "► Vérification..."
docker exec "$AIRFLOW_CONTAINER" python -c "
import openpyxl, pandas, sqlalchemy, psycopg2
print('  openpyxl  :', openpyxl.__version__)
print('  pandas    :', pandas.__version__)
print('  sqlalchemy:', sqlalchemy.__version__)
print('  psycopg2  :', psycopg2.__version__)
print('  ✓ Tout est disponible')
"

echo ""
echo "► [4/5] Création des Airflow Variables..."
docker exec "$AIRFLOW_CONTAINER" airflow variables set \
    excel_drop_folder "/opt/airflow/data/excel_drop"
echo "    ✓ excel_drop_folder"

docker exec "$AIRFLOW_CONTAINER" airflow variables set \
    pg_staging_conn "postgresql://airflow:airflow@postgres:5432/airflow"
echo "    ✓ pg_staging_conn"

echo ""
echo "► [5/5] Création du schéma staging dans PostgreSQL..."
docker exec data_pipeline-postgres-1 psql -U airflow -d airflow -c \
    "CREATE SCHEMA IF NOT EXISTS staging;"
echo "    ✓ Schéma staging prêt"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║              ✓ DÉPLOIEMENT COMPLET                   ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "  Prochaine étape — déposer le fichier Excel et tester :"
echo ""
echo "  docker cp Rapport_Production_1782708216471.xlsx \\"
echo "    $AIRFLOW_CONTAINER:/opt/airflow/data/excel_drop/"
echo ""
echo "  docker exec $AIRFLOW_CONTAINER \\"
echo "    airflow dags trigger dag_excel_ingestion"
echo ""
"""
dag_excel_ingestion.py
PROJET VALISOA 2026 — Ingestion des exports Excel de production
Fonctionne avec Airflow 3.x (airflow.sdk)

Structure du fichier source :
  Sheet "Realisations"        → staging.raw_excel_realisations
  Sheet "Détails Arrêts"      → staging.raw_excel_arrets
  Sheet "Détails Rebuts"      → staging.raw_excel_rebuts
  Sheet "Détails Cadences"    → staging.raw_excel_cadences
  Sheet "Détails Progressions"→ staging.raw_excel_progressions
  Sheet "Détails Changements" → staging.raw_excel_changements
"""

from __future__ import annotations

import os
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — centralisée ici, à déplacer dans Airflow Variables/Connections
# ---------------------------------------------------------------------------
EXCEL_DROP_FOLDER = Variable.get(
    "excel_drop_folder",
    default="/opt/airflow/data/excel_drop",
)
PG_CONN_STR = Variable.get(
    "pg_staging_conn",
    default="postgresql://airflow:airflow@postgres:5432/airflow",
)
STAGING_SCHEMA = "staging"

# Mapping feuille → table de destination
SHEET_MAP = {
    "Realisations":           "raw_excel_realisations",
    "Détails Arrêts":         "raw_excel_arrets",
    "Détails Rebuts":         "raw_excel_rebuts",
    "Détails Cadences":       "raw_excel_cadences",
    "Détails Progressions":   "raw_excel_progressions",
    "Détails Changements":    "raw_excel_changements",
}

# Colonnes datetime à parser par feuille
DATETIME_COLS = {
    "Realisations":           ["Date Début", "Date Fin"],
    "Détails Arrêts":         ["Date Début", "Date Fin"],
    "Détails Rebuts":         ["Date Saisie"],
    "Détails Cadences":       [],
    "Détails Progressions":   ["Date Rapport"],
    "Détails Changements":    ["Date Début", "Date Fin"],
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="dag_excel_ingestion",
    description="Ingestion des exports Excel de production → staging PostgreSQL",
    schedule="0 6 * * *",          # chaque jour à 06h00 (après dépôt nocturne)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["valisoa", "ingestion", "excel", "production"],
)
def excel_ingestion_dag():

    # -----------------------------------------------------------------------
    # TASK 1 — Découverte des fichiers Excel déposés
    # -----------------------------------------------------------------------
    @task()
    def discover_files() -> list[str]:
        """
        Parcourt le dossier de dépôt et retourne la liste des fichiers .xlsx
        non encore traités (moins de 24h ou non archivés).
        """
        drop_folder = Path(EXCEL_DROP_FOLDER)
        drop_folder.mkdir(parents=True, exist_ok=True)

        files = sorted(drop_folder.glob("*.xlsx"))
        if not files:
            logger.warning("Aucun fichier Excel trouvé dans %s", drop_folder)
            return []

        paths = [str(f) for f in files]
        logger.info("%d fichier(s) trouvé(s) : %s", len(paths), paths)
        return paths

    # -----------------------------------------------------------------------
    # TASK 2 — Création du schéma staging si besoin
    # -----------------------------------------------------------------------
    @task()
    def ensure_staging_schema():
        """
        Crée le schéma staging et toutes les tables raw_excel_*
        si elles n'existent pas encore (idempotent).
        """
        engine = create_engine(PG_CONN_STR)
        ddl_statements = [
            f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};",

            # Table de contrôle des ingestions (évite les doublons)
            f"""
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.ingestion_log (
                id              SERIAL PRIMARY KEY,
                filename        TEXT NOT NULL,
                file_hash       TEXT NOT NULL,
                sheet_name      TEXT NOT NULL,
                rows_ingested   INTEGER,
                ingested_at     TIMESTAMP DEFAULT NOW(),
                dag_run_id      TEXT,
                UNIQUE (file_hash, sheet_name)
            );
            """,

            # Réalisations (une ligne = un OF sur une machine)
            f"""
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.raw_excel_realisations (
                _id             SERIAL PRIMARY KEY,
                _source_file    TEXT,
                _ingested_at    TIMESTAMP DEFAULT NOW(),
                _dag_run_id     TEXT,
                code_of         TEXT,
                produit         TEXT,
                atelier         TEXT,
                ligne           TEXT,
                machine         TEXT,
                operateur       TEXT,
                effectif_journalier INTEGER,
                date_debut      TIMESTAMP,
                date_fin        TIMESTAMP,
                qte_prevue      INTEGER,
                qte_produite    INTEGER,
                qte_rebut       INTEGER,
                statut          TEXT
            );
            """,

            # Arrêts
            f"""
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.raw_excel_arrets (
                _id             SERIAL PRIMARY KEY,
                _source_file    TEXT,
                _ingested_at    TIMESTAMP DEFAULT NOW(),
                _dag_run_id     TEXT,
                code_of         TEXT,
                produit         TEXT,
                atelier         TEXT,
                machine         TEXT,
                date_debut      TIMESTAMP,
                date_fin        TIMESTAMP,
                duree_min       NUMERIC(10,2),
                type_arret      TEXT,
                motif           TEXT
            );
            """,

            # Rebuts
            f"""
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.raw_excel_rebuts (
                _id             SERIAL PRIMARY KEY,
                _source_file    TEXT,
                _ingested_at    TIMESTAMP DEFAULT NOW(),
                _dag_run_id     TEXT,
                code_of         TEXT,
                produit         TEXT,
                atelier         TEXT,
                machine         TEXT,
                date_saisie     TIMESTAMP,
                quantite        INTEGER,
                motif           TEXT,
                composant_cible TEXT
            );
            """,

            # Cadences & TRS
            f"""
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.raw_excel_cadences (
                _id                  SERIAL PRIMARY KEY,
                _source_file         TEXT,
                _ingested_at         TIMESTAMP DEFAULT NOW(),
                _dag_run_id          TEXT,
                code_of              TEXT,
                produit              TEXT,
                atelier              TEXT,
                machine              TEXT,
                qte_produite         INTEGER,
                duree_totale_min     NUMERIC(10,2),
                duree_arrets_min     NUMERIC(10,2),
                temps_net_min        NUMERIC(10,2),
                cadence_reelle       NUMERIC(10,4),
                cadence_theorique    NUMERIC(10,4),
                ecart_pct            NUMERIC(10,2),
                disponibilite_pct    NUMERIC(10,2),
                performance_pct      NUMERIC(10,2),
                qualite_pct          NUMERIC(10,2),
                trs_pct              NUMERIC(10,2)
            );
            """,

            # Progressions
            f"""
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.raw_excel_progressions (
                _id             SERIAL PRIMARY KEY,
                _source_file    TEXT,
                _ingested_at    TIMESTAMP DEFAULT NOW(),
                _dag_run_id     TEXT,
                code_of         TEXT,
                produit         TEXT,
                atelier         TEXT,
                machine         TEXT,
                date_rapport    TIMESTAMP,
                qte_produite    INTEGER,
                qte_rebut       INTEGER,
                cumul_prod      INTEGER,
                cumul_rebut     INTEGER
            );
            """,

            # Changements de série
            f"""
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.raw_excel_changements (
                _id             SERIAL PRIMARY KEY,
                _source_file    TEXT,
                _ingested_at    TIMESTAMP DEFAULT NOW(),
                _dag_run_id     TEXT,
                machine         TEXT,
                of_precedent    TEXT,
                of_suivant      TEXT,
                date_debut      TIMESTAMP,
                date_fin        TIMESTAMP,
                duree_min       NUMERIC(10,2)
            );
            """,
        ]
        with engine.begin() as conn:
            for stmt in ddl_statements:
                conn.execute(text(stmt))
        logger.info("Schéma staging et tables vérifiés/créés.")

    # -----------------------------------------------------------------------
    # TASK 3 — Ingestion d'un fichier Excel (appelée pour chaque fichier)
    # -----------------------------------------------------------------------
    @task()
    def ingest_excel_file(file_path: str, **context) -> dict:
        """
        Lit chaque feuille du fichier Excel, normalise les colonnes,
        vérifie les doublons via hash, et insère dans staging.
        Retourne un rapport d'ingestion.
        """
        dag_run_id = context["run_id"]
        engine = create_engine(PG_CONN_STR)
        filename = Path(file_path).name
        report = {"file": filename, "sheets": {}}

        # Calcul du hash du fichier (détection doublons)
        with open(file_path, "rb") as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()

        # Lecture de toutes les feuilles en une passe
        try:
            all_sheets = pd.read_excel(
                file_path,
                sheet_name=list(SHEET_MAP.keys()),
                dtype=str,           # tout en str d'abord, on typera après
                na_values=["", "NA", "N/A", "#N/A"],
            )
        except Exception as e:
            raise RuntimeError(f"Impossible de lire {file_path} : {e}") from e

        for sheet_name, table_name in SHEET_MAP.items():
            if sheet_name not in all_sheets:
                logger.warning("Feuille '%s' absente de %s — ignorée.", sheet_name, filename)
                continue

            df = all_sheets[sheet_name].copy()
            if df.empty:
                logger.info("Feuille '%s' vide — ignorée.", sheet_name)
                continue

            # Vérification doublon via log
            with engine.begin() as conn:
                exists = conn.execute(text(
                    f"SELECT 1 FROM {STAGING_SCHEMA}.ingestion_log "
                    f"WHERE file_hash = :h AND sheet_name = :s"
                ), {"h": file_hash, "s": sheet_name}).fetchone()
            if exists:
                logger.info("Feuille '%s' déjà ingérée (hash %s) — ignorée.", sheet_name, file_hash[:8])
                report["sheets"][sheet_name] = "skipped (duplicate)"
                continue

            # --- Nettoyage générique ---
            # Normalisation des noms de colonnes
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(r"[éèê]", "e", regex=True)
                .str.replace(r"[àâ]", "a", regex=True)
                .str.replace(r"[ùû]", "u", regex=True)
                .str.replace(r"[îï]", "i", regex=True)
                .str.replace(r"[ôö]", "o", regex=True)
                .str.replace(r"\s+", "_", regex=True)
                .str.replace(r"[^\w]", "_", regex=True)
                .str.strip("_")
            )

            # Suppression des lignes entièrement vides
            df = df.dropna(how="all")

            # Renommage explicite des colonnes ambiguës ou mal normalisées
            # Problème : "Durée (min)" → normalisation générique → "duree__min" (double __)
            # Problème : "Type" → mot réservé PostgreSQL → renommer en "type_arret"
            col_renames = {
                # Feuille Arrêts
                "duree__min":                    "duree_min",
                "type":                          "type_arret",
                # Feuille Cadences — patterns réels observés en production
                "duree_totale__min":             "duree_totale_min",
                "duree_totale__min_":            "duree_totale_min",
                "duree_arrets__min":             "duree_arrets_min",
                "duree_arrets__min_":            "duree_arrets_min",
                "temps_net__min":                "temps_net_min",
                "temps_net__min_":               "temps_net_min",
                "cadence_reelle__pcs_min":       "cadence_reelle",
                "cadence_reelle__pcs_min_":      "cadence_reelle",
                "cadence_theorique__pcs_min":    "cadence_theorique",
                "cadence_theorique__pcs_min_":   "cadence_theorique",
                "ecart___":                      "ecart_pct",
                "ecart":                         "ecart_pct",
                "disponibilite":                 "disponibilite_pct",
                "performance":                   "performance_pct",
                "qualite":                       "qualite_pct",
                "trs":                           "trs_pct",
            }
            df.rename(columns={k: v for k, v in col_renames.items() if k in df.columns}, inplace=True)
            logger.info("Colonnes après renommage : %s", list(df.columns))

            # Parsing des colonnes datetime
            for col_original in DATETIME_COLS.get(sheet_name, []):
                col_norm = (
                    col_original.lower()
                    .replace("é", "e").replace("è", "e").replace("ê", "e")
                    .replace("à", "a").replace("â", "a")
                    .replace(" ", "_").replace("(", "_").replace(")", "_")
                    .strip("_")
                )
                if col_norm in df.columns:
                    df[col_norm] = pd.to_datetime(
                        df[col_norm], dayfirst=True, errors="coerce"
                    )

            # Colonnes numériques (%, cadences, durées)
            numeric_keywords = ["pct", "min", "qte", "quantite", "duree",
                                 "effectif", "cadence", "ecart", "cumul"]
            for col in df.columns:
                if any(kw in col for kw in numeric_keywords):
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Métadonnées de traçabilité
            df["_source_file"] = filename
            df["_dag_run_id"] = dag_run_id

            # --- Insertion en base ---
            rows = len(df)
            try:
                df.to_sql(
                    name=table_name,
                    con=engine,
                    schema=STAGING_SCHEMA,
                    if_exists="append",
                    index=False,
                    method="multi",     # batch insert
                    chunksize=500,
                )
            except Exception as e:
                raise RuntimeError(
                    f"Erreur insertion {sheet_name} → {STAGING_SCHEMA}.{table_name} : {e}"
                ) from e

            # Enregistrement dans le log
            with engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO {STAGING_SCHEMA}.ingestion_log
                        (filename, file_hash, sheet_name, rows_ingested, dag_run_id)
                    VALUES (:fn, :h, :s, :r, :d)
                    ON CONFLICT (file_hash, sheet_name) DO NOTHING
                """), {
                    "fn": filename, "h": file_hash,
                    "s": sheet_name, "r": rows, "d": dag_run_id
                })

            logger.info("✓ %s — '%s' : %d lignes → %s.%s", filename, sheet_name, rows, STAGING_SCHEMA, table_name)
            report["sheets"][sheet_name] = f"{rows} rows ingested"

        return report

    # -----------------------------------------------------------------------
    # TASK 4 — Archivage des fichiers traités
    # -----------------------------------------------------------------------
    @task()
    def archive_files(file_paths: list[str]):
        """
        Déplace les fichiers traités vers un sous-dossier /archive/YYYY-MM-DD/
        pour ne pas les réingérer au prochain run.
        """
        if not file_paths:
            return

        today = datetime.now().strftime("%Y-%m-%d")
        for file_path in file_paths:
            src = Path(file_path)
            if not src.exists():
                continue
            archive_dir = src.parent / "archive" / today
            archive_dir.mkdir(parents=True, exist_ok=True)
            dest = archive_dir / src.name
            src.rename(dest)
            logger.info("Archivé : %s → %s", src.name, dest)

    # -----------------------------------------------------------------------
    # TASK 5 — Rapport de synthèse
    # -----------------------------------------------------------------------
    @task()
    def log_summary(reports: list[dict]):
        """Log un résumé de l'ingestion pour le monitoring Airflow."""
        total_files = len(reports)
        total_sheets = sum(len(r.get("sheets", {})) for r in reports)
        logger.info("=" * 60)
        logger.info("RÉSUMÉ INGESTION EXCEL — %d fichier(s), %d feuille(s)", total_files, total_sheets)
        for r in reports:
            logger.info("  📄 %s", r.get("file", "?"))
            for sheet, status in r.get("sheets", {}).items():
                logger.info("      └─ %s : %s", sheet, status)
        logger.info("=" * 60)

    # -----------------------------------------------------------------------
    # TASK — Court-circuit si aucun fichier trouvé
    # -----------------------------------------------------------------------
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def has_files(file_paths: list[str]) -> bool:
        """
        Retourne True si des fichiers sont à traiter, False sinon.
        En cas de False, les tasks ingest/archive/log_summary sont skippées
        mais trigger_quality_check s'exécute quand même grâce à
        ignore_downstream_trigger_rules=False + TriggerRule.ALL_DONE.
        """
        if not file_paths:
            logger.info("Aucun fichier Excel à ingérer — pipeline court-circuité.")
            logger.info("Le DAG quality sera quand même déclenché sur les données existantes.")
            return False
        logger.info("%d fichier(s) à traiter.", len(file_paths))
        return True

    # -----------------------------------------------------------------------
    # Orchestration du DAG
    # -----------------------------------------------------------------------
    from airflow.utils.trigger_rule import TriggerRule

    schema_ready = ensure_staging_schema()
    files = discover_files()
    gate = has_files(file_paths=files)

    # Ingestion de chaque fichier (dynamic task mapping) — skippée si gate=False
    reports = ingest_excel_file.expand(file_path=files)
    archived = archive_files(file_paths=files)
    summary = log_summary(reports=reports)

    # Déclenchement quality — toujours exécuté (ALL_DONE = même si upstream skipped)
    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_check",
        trigger_dag_id="dag_excel_quality",
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Dépendances
    schema_ready >> files >> gate >> reports >> archived >> summary >> trigger_quality


excel_ingestion_dag()
"""
dag_excel_quality.py
PROJET VALISOA 2026 — Couche Data Quality (Bronze → Quality → Silver)

Architecture :
  staging.*        → règles qualité (4 familles)
  ├── validated.*  → données conformes (Silver)
  └── quarantine.* → données non conformes (isolées, corrigibles)
  quality.*        → rapports et logs de qualité

Familles de règles :
  QA — Intégrité structurelle
  QB — Cohérence temporelle
  QC — Cohérence métier (dont TRS 80-120%)
  QD — Complétude

Sévérités :
  CRITICAL → quarantaine immédiate
  WARNING  → quarantaine avec log
  INFO     → validated avec flag
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import create_engine, text

from airflow.sdk import dag, task
from airflow.sdk import Variable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG_CONN_STR = Variable.get(
    "pg_staging_conn",
    default_var="postgresql://airflow:airflow@postgres:5432/airflow",
)
STAGING_SCHEMA   = "staging"
QUALITY_SCHEMA   = "quality"
VALIDATED_SCHEMA = "validated"
QUARANTINE_SCHEMA = "quarantine"

# Seuils TRS et composantes (ajustables via Airflow Variables)
TRS_MIN_WARNING  = float(Variable.get("trs_min_warning",  default_var="80"))
TRS_MAX_INFO     = float(Variable.get("trs_max_info",     default_var="100"))
TRS_MAX_CRITICAL = float(Variable.get("trs_max_critical", default_var="120"))
QUALITE_MIN_WARNING = float(Variable.get("qualite_min_warning", default_var="95"))

# ---------------------------------------------------------------------------
# Règles de qualité par table
# Chaque règle : (code, famille, sévérité, description, expression SQL)
# L'expression SQL doit retourner TRUE si la règle est VIOLÉE
# ---------------------------------------------------------------------------
QUALITY_RULES = {

    "raw_excel_realisations": [
        # Famille A — Intégrité structurelle
        ("QA001", "A", "CRITICAL", "code_of null ou vide",
         "code_of IS NULL OR TRIM(code_of) = ''"),
        ("QA002", "A", "CRITICAL", "date_debut null",
         "date_debut IS NULL"),
        ("QA003", "A", "CRITICAL", "qte_prevue négative",
         "qte_prevue IS NOT NULL AND qte_prevue < 0"),
        ("QA004", "A", "CRITICAL", "qte_produite négative",
         "qte_produite IS NOT NULL AND qte_produite < 0"),
        # Famille B — Cohérence temporelle
        ("QB001", "B", "CRITICAL", "date_fin antérieure à date_debut",
         "date_fin IS NOT NULL AND date_debut IS NOT NULL AND date_fin < date_debut"),
        ("QB003", "B", "WARNING",  "date_debut hors plage 2020-demain",
         "date_debut IS NOT NULL AND (date_debut < '2020-01-01' OR date_debut > NOW() + INTERVAL '1 day')"),
        # Famille C — Cohérence métier
        ("QC001", "C", "WARNING",  "qte_produite > 3× qte_prevue (surproduction suspecte)",
         "qte_produite IS NOT NULL AND qte_prevue IS NOT NULL AND qte_prevue > 0 AND qte_produite > qte_prevue * 3"),
        ("QC002", "C", "CRITICAL", "qte_rebut > qte_produite",
         "qte_rebut IS NOT NULL AND qte_produite IS NOT NULL AND qte_rebut > qte_produite"),
        # Famille D — Complétude
        ("QD001", "D", "CRITICAL", "atelier null ou vide",
         "atelier IS NULL OR TRIM(atelier) = ''"),
        ("QD002", "D", "CRITICAL", "machine null ou vide",
         "machine IS NULL OR TRIM(machine) = ''"),
        ("QD003", "D", "WARNING",  "produit null ou vide",
         "produit IS NULL OR TRIM(produit) = ''"),
    ],

    "raw_excel_arrets": [
        ("QA005", "A", "CRITICAL", "code_of null ou vide",
         "code_of IS NULL OR TRIM(code_of) = ''"),
        ("QA006", "A", "WARNING",  "duree_min négative",
         "duree_min IS NOT NULL AND duree_min < 0"),
        ("QB002", "B", "CRITICAL", "date_fin antérieure à date_debut",
         "date_fin IS NOT NULL AND date_debut IS NOT NULL AND date_fin < date_debut"),
        ("QB005", "B", "WARNING",  "date_debut null",
         "date_debut IS NULL"),
        ("QC007", "C", "WARNING",  "durée arrêt > 24h (1440 min)",
         "duree_min IS NOT NULL AND duree_min > 1440"),
        ("QD004", "D", "WARNING",  "motif arrêt null ou vide",
         "motif IS NULL OR TRIM(motif) = ''"),
        ("QD006", "D", "WARNING",  "type_arret null ou vide",
         "type_arret IS NULL OR TRIM(type_arret) = ''"),
    ],

    "raw_excel_rebuts": [
        ("QA007", "A", "CRITICAL", "code_of null ou vide",
         "code_of IS NULL OR TRIM(code_of) = ''"),
        ("QA008", "A", "CRITICAL", "quantite négative ou nulle",
         "quantite IS NOT NULL AND quantite <= 0"),
        ("QD005", "D", "WARNING",  "motif rebut null ou vide",
         "motif IS NULL OR TRIM(motif) = ''"),
        ("QD007", "D", "INFO",     "composant_cible non renseigné",
         "composant_cible IS NULL OR TRIM(composant_cible) = ''"),
    ],

    "raw_excel_cadences": [
        # Règles TRS — seuils métier VALISOA (80%-120%)
        ("QC003", "C", "CRITICAL", f"TRS > {TRS_MAX_CRITICAL}% (aberration technique)",
         f"trs_pct IS NOT NULL AND trs_pct > {TRS_MAX_CRITICAL}"),
        ("QC003B", "C", "CRITICAL", "TRS négatif",
         "trs_pct IS NOT NULL AND trs_pct < 0"),
        ("QC003C", "C", "WARNING",  f"TRS < {TRS_MIN_WARNING}% (sous-performance)",
         f"trs_pct IS NOT NULL AND trs_pct >= 0 AND trs_pct < {TRS_MIN_WARNING}"),
        ("QC003D", "C", "INFO",     f"TRS entre {TRS_MAX_INFO}% et {TRS_MAX_CRITICAL}% (sur-performance à vérifier)",
         f"trs_pct IS NOT NULL AND trs_pct > {TRS_MAX_INFO} AND trs_pct <= {TRS_MAX_CRITICAL}"),
        # Composantes TRS
        ("QC004", "C", "CRITICAL", "Disponibilité > 120% (aberration)",
         f"disponibilite_pct IS NOT NULL AND disponibilite_pct > {TRS_MAX_CRITICAL}"),
        ("QC004B", "C", "WARNING",  f"Disponibilité < {TRS_MIN_WARNING}%",
         f"disponibilite_pct IS NOT NULL AND disponibilite_pct < {TRS_MIN_WARNING}"),
        ("QC005", "C", "CRITICAL", "Performance > 120% (aberration)",
         f"performance_pct IS NOT NULL AND performance_pct > {TRS_MAX_CRITICAL}"),
        ("QC005B", "C", "WARNING",  f"Performance < {TRS_MIN_WARNING}%",
         f"performance_pct IS NOT NULL AND performance_pct < {TRS_MIN_WARNING}"),
        ("QC006", "C", "CRITICAL", "Qualité > 120% (aberration)",
         f"qualite_pct IS NOT NULL AND qualite_pct > {TRS_MAX_CRITICAL}"),
        ("QC006B", "C", "WARNING",  f"Qualité < {QUALITE_MIN_WARNING}% (taux rebut élevé)",
         f"qualite_pct IS NOT NULL AND qualite_pct < {QUALITE_MIN_WARNING}"),
        # Intégrité
        ("QA009", "A", "CRITICAL", "code_of null ou vide",
         "code_of IS NULL OR TRIM(code_of) = ''"),
        ("QA010", "A", "WARNING",  "qte_produite négative",
         "qte_produite IS NOT NULL AND qte_produite < 0"),
    ],

    "raw_excel_progressions": [
        ("QA011", "A", "CRITICAL", "code_of null ou vide",
         "code_of IS NULL OR TRIM(code_of) = ''"),
        ("QA012", "A", "WARNING",  "qte_produite négative",
         "qte_produite IS NOT NULL AND qte_produite < 0"),
        ("QC008", "C", "WARNING",  "cumul_rebut > cumul_prod",
         "cumul_rebut IS NOT NULL AND cumul_prod IS NOT NULL AND cumul_rebut > cumul_prod"),
    ],

    "raw_excel_changements": [
        ("QB004", "B", "CRITICAL", "date_fin antérieure à date_debut",
         "date_fin IS NOT NULL AND date_debut IS NOT NULL AND date_fin < date_debut"),
        ("QA013", "A", "WARNING",  "machine null ou vide",
         "machine IS NULL OR TRIM(machine) = ''"),
        ("QC009", "C", "WARNING",  "durée changement > 8h (480 min)",
         "duree_min IS NOT NULL AND duree_min > 480"),
    ],
}

# Colonnes à exclure lors de la copie vers validated/quarantine
META_COLS = {"_id", "_ingested_at"}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="dag_excel_quality",
    description="Data Quality — Bronze → validated (Silver) + quarantine",
    schedule=None,   # déclenché uniquement par dag_excel_ingestion
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["valisoa", "quality", "silver", "governance"],
)
def excel_quality_dag():

    # -----------------------------------------------------------------------
    # TASK 1 — Création des schémas et tables de contrôle
    # -----------------------------------------------------------------------
    @task()
    def ensure_quality_schemas():
        """
        Crée les schémas quality, validated, quarantine et leurs tables
        de gouvernance. Entièrement idempotent.
        """
        engine = create_engine(PG_CONN_STR)
        ddl = [
            f"CREATE SCHEMA IF NOT EXISTS {QUALITY_SCHEMA};",
            f"CREATE SCHEMA IF NOT EXISTS {VALIDATED_SCHEMA};",
            f"CREATE SCHEMA IF NOT EXISTS {QUARANTINE_SCHEMA};",

            # Table de résultats règle par règle
            f"""
            CREATE TABLE IF NOT EXISTS {QUALITY_SCHEMA}.check_results (
                id              SERIAL PRIMARY KEY,
                dag_run_id      TEXT NOT NULL,
                source_table    TEXT NOT NULL,
                source_row_id   INTEGER NOT NULL,
                rule_code       TEXT NOT NULL,
                rule_family     TEXT NOT NULL,
                severity        TEXT NOT NULL,
                description     TEXT,
                is_violated     BOOLEAN NOT NULL,
                checked_at      TIMESTAMP DEFAULT NOW()
            );
            """,

            # Index pour les requêtes de reporting
            f"""
            CREATE INDEX IF NOT EXISTS idx_check_results_run
                ON {QUALITY_SCHEMA}.check_results (dag_run_id, source_table);
            """,

            # Log de synthèse par run et par table
            f"""
            CREATE TABLE IF NOT EXISTS {QUALITY_SCHEMA}.run_log (
                id                  SERIAL PRIMARY KEY,
                dag_run_id          TEXT NOT NULL,
                source_table        TEXT NOT NULL,
                rows_checked        INTEGER,
                rows_valid          INTEGER,
                rows_warning        INTEGER,
                rows_critical       INTEGER,
                rows_quarantined    INTEGER,
                quality_rate_pct    NUMERIC(5,2),
                run_at              TIMESTAMP DEFAULT NOW(),
                UNIQUE (dag_run_id, source_table)
            );
            """,
        ]
        with engine.begin() as conn:
            for stmt in ddl:
                conn.execute(text(stmt))
        logger.info("Schémas quality / validated / quarantine vérifiés.")

    # -----------------------------------------------------------------------
    # TASK 2 — Contrôle qualité d'une table (générique)
    # -----------------------------------------------------------------------
    @task()
    def check_table(table_name: str, **context) -> dict[str, Any]:
        """
        Applique toutes les règles qualité sur une table staging.
        Écrit les résultats dans quality.check_results.
        Retourne les stats pour le routage.
        """
        dag_run_id = context["run_id"]
        engine = create_engine(PG_CONN_STR)
        rules = QUALITY_RULES.get(table_name, [])

        if not rules:
            logger.warning("Aucune règle définie pour %s", table_name)
            return {"table": table_name, "rows_checked": 0}

        # Vérifier que la table source existe et a des données
        with engine.connect() as conn:
            count = conn.execute(text(
                f"SELECT COUNT(*) FROM {STAGING_SCHEMA}.{table_name}"
            )).scalar()

        if count == 0:
            logger.info("Table %s vide — contrôle ignoré.", table_name)
            return {"table": table_name, "rows_checked": 0}

        logger.info("Contrôle qualité de %s — %d lignes, %d règles",
                    table_name, count, len(rules))

        violations: dict[int, list] = {}  # row_id → liste de violations

        with engine.begin() as conn:
            for rule_code, family, severity, description, sql_expr in rules:
                # Récupère les IDs des lignes qui violent la règle
                violated_rows = conn.execute(text(
                    f"SELECT _id FROM {STAGING_SCHEMA}.{table_name} WHERE {sql_expr}"
                )).fetchall()

                violated_ids = [r[0] for r in violated_rows]

                # Insertion en batch dans check_results
                if violated_ids:
                    conn.execute(text(f"""
                        INSERT INTO {QUALITY_SCHEMA}.check_results
                            (dag_run_id, source_table, source_row_id,
                             rule_code, rule_family, severity, description, is_violated)
                        SELECT
                            :run_id, :tbl, _id,
                            :rc, :fam, :sev, :desc, TRUE
                        FROM {STAGING_SCHEMA}.{table_name}
                        WHERE _id = ANY(:ids)
                        ON CONFLICT DO NOTHING
                    """), {
                        "run_id": dag_run_id, "tbl": table_name,
                        "rc": rule_code, "fam": family,
                        "sev": severity, "desc": description,
                        "ids": violated_ids,
                    })

                    for row_id in violated_ids:
                        if row_id not in violations:
                            violations[row_id] = []
                        violations[row_id].append((rule_code, severity))

                logger.info("  %s [%s] %s — %d violation(s)",
                            rule_code, severity, description[:50], len(violated_ids))

        # Calcul des statistiques
        critical_rows = {rid for rid, viols in violations.items()
                         if any(s == "CRITICAL" for _, s in viols)}
        warning_rows  = {rid for rid, viols in violations.items()
                         if any(s == "WARNING"  for _, s in viols)
                         and rid not in critical_rows}
        quarantine_rows = critical_rows | warning_rows
        valid_rows = count - len(quarantine_rows)
        quality_rate = round(valid_rows / count * 100, 2) if count > 0 else 0

        # Enregistrement dans run_log
        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {QUALITY_SCHEMA}.run_log
                    (dag_run_id, source_table, rows_checked, rows_valid,
                     rows_warning, rows_critical, rows_quarantined, quality_rate_pct)
                VALUES (:rid, :tbl, :chk, :val, :wrn, :crt, :qrt, :qr)
                ON CONFLICT (dag_run_id, source_table) DO UPDATE SET
                    rows_checked     = EXCLUDED.rows_checked,
                    rows_valid       = EXCLUDED.rows_valid,
                    rows_warning     = EXCLUDED.rows_warning,
                    rows_critical    = EXCLUDED.rows_critical,
                    rows_quarantined = EXCLUDED.rows_quarantined,
                    quality_rate_pct = EXCLUDED.quality_rate_pct,
                    run_at           = NOW()
            """), {
                "rid": dag_run_id, "tbl": table_name,
                "chk": count, "val": valid_rows,
                "wrn": len(warning_rows), "crt": len(critical_rows),
                "qrt": len(quarantine_rows), "qr": quality_rate,
            })

        logger.info("Table %s — taux qualité : %.1f%% (%d/%d lignes valides)",
                    table_name, quality_rate, valid_rows, count)

        return {
            "table":             table_name,
            "rows_checked":      count,
            "rows_valid":        valid_rows,
            "rows_quarantined":  len(quarantine_rows),
            "critical_ids":      list(critical_rows),
            "warning_ids":       list(warning_rows),
            "quality_rate":      quality_rate,
        }

    # -----------------------------------------------------------------------
    # TASK 3 — Routage : validated ou quarantine
    # -----------------------------------------------------------------------
    @task()
    def route_data(check_result: dict[str, Any], **context) -> dict:
        """
        Selon les résultats de check_table :
        - Lignes valides + INFO  → validated.<table>
        - Lignes CRITICAL/WARNING → quarantine.<table>
        Crée les tables de destination à la volée si nécessaire.
        """
        dag_run_id = context["run_id"]
        table_name = check_result["table"]

        if check_result["rows_checked"] == 0:
            return {"table": table_name, "status": "skipped"}

        engine = create_engine(PG_CONN_STR)
        critical_ids = check_result.get("critical_ids", [])
        warning_ids  = check_result.get("warning_ids",  [])
        quarantine_ids = list(set(critical_ids + warning_ids))

        # --- Création des tables validated et quarantine à la volée ---
        # On copie la structure exacte de la table staging
        with engine.begin() as conn:
            # Table validated (données conformes + métadonnées qualité)
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {VALIDATED_SCHEMA}.{table_name}
                AS SELECT *, 
                    NULL::FLOAT     AS quality_score,
                    NULL::TEXT      AS quality_flags,
                    NOW()           AS validated_at,
                    ''::TEXT        AS validated_dag_run_id
                FROM {STAGING_SCHEMA}.{table_name} WHERE FALSE;
            """))

            # Table quarantine (données invalides + détail des règles)
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {QUARANTINE_SCHEMA}.{table_name}
                AS SELECT *,
                    NULL::TEXT[]    AS failed_rules,
                    NULL::TEXT      AS severity_max,
                    NOW()           AS quarantined_at,
                    ''::TEXT        AS quarantine_dag_run_id
                FROM {STAGING_SCHEMA}.{table_name} WHERE FALSE;
            """))

        # --- Insertion dans validated ---
        valid_filter = (
            f"_id != ALL(ARRAY{quarantine_ids}::int[])"
            if quarantine_ids else "TRUE"
        )

        # Calcul du quality_score par ligne (ratio règles passées / total règles)
        total_rules = len(QUALITY_RULES.get(table_name, []))

        with engine.begin() as conn:
            # Compter les violations INFO par ligne pour le score
            rows_inserted = conn.execute(text(f"""
                INSERT INTO {VALIDATED_SCHEMA}.{table_name}
                SELECT s.*,
                    ROUND(
                        (1.0 - COALESCE(v.violations, 0)::FLOAT / NULLIF(:total_rules, 0)) * 100,
                        2
                    )                                   AS quality_score,
                    COALESCE(v.flags, 'VALID')          AS quality_flags,
                    NOW()                               AS validated_at,
                    :run_id                             AS validated_dag_run_id
                FROM {STAGING_SCHEMA}.{table_name} s
                LEFT JOIN (
                    SELECT source_row_id,
                           COUNT(*)            AS violations,
                           STRING_AGG(rule_code, ', ' ORDER BY rule_code) AS flags
                    FROM {QUALITY_SCHEMA}.check_results
                    WHERE dag_run_id = :run_id
                      AND source_table = :tbl
                      AND severity = 'INFO'
                    GROUP BY source_row_id
                ) v ON v.source_row_id = s._id
                WHERE {valid_filter}
                ON CONFLICT DO NOTHING
            """), {"run_id": dag_run_id, "tbl": table_name,
                   "total_rules": total_rules})
            valid_count = rows_inserted.rowcount

        # --- Insertion dans quarantine ---
        quarantine_count = 0
        if quarantine_ids:
            with engine.begin() as conn:
                rows_qrt = conn.execute(text(f"""
                    INSERT INTO {QUARANTINE_SCHEMA}.{table_name}
                    SELECT s.*,
                        ARRAY_AGG(DISTINCT qr.rule_code)  AS failed_rules,
                        MAX(qr.severity)                  AS severity_max,
                        NOW()                             AS quarantined_at,
                        :run_id                           AS quarantine_dag_run_id
                    FROM {STAGING_SCHEMA}.{table_name} s
                    JOIN {QUALITY_SCHEMA}.check_results qr
                        ON qr.source_row_id = s._id
                       AND qr.dag_run_id    = :run_id
                       AND qr.source_table  = :tbl
                       AND qr.is_violated   = TRUE
                       AND qr.severity IN ('CRITICAL', 'WARNING')
                    WHERE s._id = ANY(ARRAY{quarantine_ids}::int[])
                    GROUP BY {', '.join([f's.{c}' for c in _get_columns(engine, STAGING_SCHEMA, table_name)])}
                    ON CONFLICT DO NOTHING
                """), {"run_id": dag_run_id, "tbl": table_name})
                quarantine_count = rows_qrt.rowcount

        logger.info("Route %s — validated: %d | quarantine: %d",
                    table_name, valid_count, quarantine_count)
        return {
            "table": table_name,
            "validated": valid_count,
            "quarantined": quarantine_count,
        }

    # -----------------------------------------------------------------------
    # TASK 4 — Rapport de synthèse qualité
    # -----------------------------------------------------------------------
    @task()
    def quality_report_summary(route_results: list[dict], **context):
        """
        Produit un rapport de qualité global dans les logs Airflow.
        Alerte si le taux qualité global passe sous le seuil critique (90%).
        """
        dag_run_id = context["run_id"]
        engine = create_engine(PG_CONN_STR)

        with engine.connect() as conn:
            results = conn.execute(text(f"""
                SELECT
                    source_table,
                    rows_checked,
                    rows_valid,
                    rows_quarantined,
                    quality_rate_pct
                FROM {QUALITY_SCHEMA}.run_log
                WHERE dag_run_id = :rid
                ORDER BY quality_rate_pct ASC
            """), {"rid": dag_run_id}).fetchall()

        logger.info("=" * 65)
        logger.info("RAPPORT DATA QUALITY — VALISOA 2026")
        logger.info("Run ID : %s", dag_run_id)
        logger.info("=" * 65)
        logger.info("%-35s %8s %8s %8s %8s",
                    "Table", "Vérif.", "Valides", "Qrntne", "Qualité%")
        logger.info("-" * 65)

        global_checked = 0
        global_valid   = 0
        critical_tables = []

        for row in results:
            tbl, checked, valid, quarantined, rate = row
            global_checked += checked or 0
            global_valid   += valid   or 0
            flag = "⚠️" if (rate or 0) < 90 else "✓"
            logger.info("%-35s %8d %8d %8d %7.1f%% %s",
                        tbl, checked or 0, valid or 0,
                        quarantined or 0, rate or 0, flag)
            if (rate or 0) < 90:
                critical_tables.append(tbl)

        global_rate = round(global_valid / global_checked * 100, 2) if global_checked > 0 else 0
        logger.info("=" * 65)
        logger.info("TOTAL : %d vérifiées | %d valides | taux global : %.1f%%",
                    global_checked, global_valid, global_rate)

        if critical_tables:
            logger.warning("⚠️  TABLES SOUS LE SEUIL 90%% : %s", ", ".join(critical_tables))
        else:
            logger.info("✓ Toutes les tables dépassent le seuil qualité de 90%%")

        logger.info("=" * 65)

        # Requête TRS aberrants pour rapport spécifique
        with engine.connect() as conn:
            trs_anomalies = conn.execute(text(f"""
                SELECT source_row_id, rule_code, description
                FROM {QUALITY_SCHEMA}.check_results
                WHERE dag_run_id   = :rid
                  AND source_table = 'raw_excel_cadences'
                  AND rule_code    LIKE 'QC003%'
                  AND is_violated  = TRUE
                ORDER BY severity DESC
            """), {"rid": dag_run_id}).fetchall()

        if trs_anomalies:
            logger.warning("📊 ANOMALIES TRS détectées (%d):", len(trs_anomalies))
            for row_id, code, desc in trs_anomalies:
                logger.warning("   Ligne %d — [%s] %s", row_id, code, desc)


    # -----------------------------------------------------------------------
    # Fonction utilitaire (hors task)
    # -----------------------------------------------------------------------
    def _get_columns(engine, schema: str, table: str) -> list[str]:
        """Retourne la liste des colonnes d'une table PostgreSQL."""
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :s AND table_name = :t
                ORDER BY ordinal_position
            """), {"s": schema, "t": table}).fetchall()
        return [r[0] for r in rows]

    # -----------------------------------------------------------------------
    # Orchestration
    # -----------------------------------------------------------------------
    tables = list(QUALITY_RULES.keys())

    schemas_ready = ensure_quality_schemas()

    check_results = [
        check_table.override(task_id=f"check_{t.replace('raw_excel_', '')}")(t)
        for t in tables
    ]

    route_results = [
        route_data.override(task_id=f"route_{t.replace('raw_excel_', '')}")(cr)
        for t, cr in zip(tables, check_results)
    ]

    summary = quality_report_summary(route_results=route_results)

    schemas_ready >> check_results
    for cr, rr in zip(check_results, route_results):
        cr >> rr
    route_results >> summary


excel_quality_dag()
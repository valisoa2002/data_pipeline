
  
    

  create  table "airflow"."gold"."dim_temps__dbt_tmp"
  
  
    as
  
  (
    -- Calendrier généré dynamiquement entre la date min et max observées
-- dans les faits (réalisations + arrêts), avec une marge de 7 jours
-- de part et d'autre pour anticiper les prochains chargements.
--
-- NOTE : marge réduite de 30 à 7 jours (2026-07-...) — une marge trop
-- large créait un écart important entre MAX(dim_temps.date) et la
-- dernière date réelle avec données, piégeant les mesures DAX qui se
-- basent sur MAX(dim_temps[date]) pour calculer des fenêtres glissantes
-- (ex: sparklines des N derniers jours). Les mesures DAX doivent malgré
-- tout rester défensives (filtrer sur NOT ISBLANK(...)) plutôt que de
-- dépendre uniquement de cette marge réduite.

with bounds as (
    select
        min(date_debut) as min_date,
        max(coalesce(date_fin, date_debut)) as max_date
    from (
        select date_debut, date_fin from "airflow"."gold_staging"."stg_realisations"
        union all
        select date_debut, date_fin from "airflow"."gold_staging"."stg_arrets"
    ) all_dates
),

spine as (
    select
        generate_series(
            (select date_trunc('day', min_date) - interval '7 day' from bounds),
            (select date_trunc('day', max_date) + interval '7 day' from bounds),
            interval '1 day'
        )::date as date_jour
)

select
    to_char(date_jour, 'YYYYMMDD')::int   as temps_key,
    date_jour                              as date,
    extract(isoyear from date_jour)::int   as annee,
    extract(month from date_jour)::int     as mois,
    to_char(date_jour, 'Month')            as nom_mois,
    extract(week from date_jour)::int      as semaine,
    extract(isodow from date_jour)::int    as jour_semaine_num,
    to_char(date_jour, 'Day')              as jour_semaine_nom,
    extract(isodow from date_jour) in (6, 7) as est_weekend
from spine
  );
  
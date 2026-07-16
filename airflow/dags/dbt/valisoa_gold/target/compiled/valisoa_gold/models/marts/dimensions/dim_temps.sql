-- Calendrier généré dynamiquement entre la date min et max observées
-- dans les faits (réalisations + arrêts), avec une marge de 30 jours
-- de part et d'autre pour anticiper les prochains chargements.

with bounds as (
    select
        min(date_debut) as min_date,
        max(coalesce(date_fin, date_debut)) as max_date
    from (
        select date_debut, date_fin from "airflow"."gold_gold_staging"."stg_realisations"
        union all
        select date_debut, date_fin from "airflow"."gold_gold_staging"."stg_arrets"
    ) all_dates
),

spine as (
    select
        generate_series(
            (select date_trunc('day', min_date) - interval '30 day' from bounds),
            (select date_trunc('day', max_date) + interval '30 day' from bounds),
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
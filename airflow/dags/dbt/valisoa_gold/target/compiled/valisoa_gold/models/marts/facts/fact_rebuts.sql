

select
    
    md5(
        
            coalesce(cast(sr.source_id as text), '')
    )
         as rebut_key,
    sr.code_of,
    sr.composant_cible,
    sr.motif,
    dt.temps_key,
    sr.quantite,
    sr.date_saisie
from "airflow"."gold_staging"."stg_rebuts" sr
left join "airflow"."gold"."dim_temps" dt
    on dt.date = date_trunc('day', sr.date_saisie)::date


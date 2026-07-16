
  create view "airflow"."gold_gold_staging"."stg_arrets__dbt_tmp"
    
    
  as (
    select
    _id                         as source_id,
    trim(code_of)                as code_of,
    trim(machine)                as machine,
    trim(motif)                  as motif,
    trim(type_arret)             as type_arret,
    date_debut,
    date_fin,
    duree_min,
    validated_at
from "airflow"."validated"."raw_excel_arrets"
  );
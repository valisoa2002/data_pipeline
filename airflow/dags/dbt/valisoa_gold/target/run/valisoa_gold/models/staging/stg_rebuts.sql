
  create view "airflow"."gold_gold_staging"."stg_rebuts__dbt_tmp"
    
    
  as (
    select
    _id                         as source_id,
    trim(code_of)                as code_of,
    trim(motif)                  as motif,
    trim(composant_cible)        as composant_cible,
    quantite,
    validated_at                 as date_saisie
from "airflow"."validated"."raw_excel_rebuts"
  );
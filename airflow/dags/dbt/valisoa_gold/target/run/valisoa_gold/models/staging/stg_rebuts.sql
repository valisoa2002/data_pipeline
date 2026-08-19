
  create view "airflow"."gold_staging"."stg_rebuts__dbt_tmp"
    
    
  as (
    select
    _id                         as source_id,
    trim(code_of)                as code_of,
    trim(motif)                  as motif,
    trim(composant_cible)        as composant_cible,
    quantite,
    date_saisie                  as date_saisie,   -- ✅ la vraie colonne Excel
    validated_at                 as date_validation  -- garder l'ancienne, renommée, utile pour l'audit/traçabilité
from "airflow"."validated"."raw_excel_rebuts"
  );
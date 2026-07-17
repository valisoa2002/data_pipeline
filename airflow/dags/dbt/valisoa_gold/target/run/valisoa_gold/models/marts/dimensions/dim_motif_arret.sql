
  
    

  create  table "airflow"."gold"."dim_motif_arret__dbt_tmp"
  
  
    as
  
  (
    -- Une ligne par combinaison distincte (type_arret, motif).
-- Le flag "planifié" est déduit par mot-clé sur le type_arret — à
-- affiner avec la nomenclature métier réelle si elle diverge.

with motifs as (
    select distinct
        coalesce(type_arret, 'NON_RENSEIGNE') as type_arret,
        coalesce(motif, 'NON_RENSEIGNE')       as motif
    from "airflow"."gold_staging"."stg_arrets"
)

select
    
    md5(
        
            coalesce(cast(type_arret as text), '') || '||' || 
            coalesce(cast(motif as text), '')
    )
   as motif_key,
    type_arret,
    motif,
    case
        when lower(type_arret) like '%planifi%'
          or lower(type_arret) like '%maintenance preventive%'
          or lower(type_arret) like '%changement%'
            then true
        else false
    end as est_planifie
from motifs
  );
  
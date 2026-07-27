
      
  
    

  create  table "airflow"."gold"."fact_rebuts__dbt_tmp"
  
  
    as
  
  (
    

-- Grain : une saisie de rebut. Jointe à machine/produit via code_of
-- (résolu depuis int_production_cadence, qui porte déjà machine et
-- produit_code pour l'OF correspondant), et à temps via date_saisie.

select
    
    md5(
        
            coalesce(cast(sr.source_id as text), '')
    )
         as rebut_key,
    sr.code_of,
    dm.machine_key,
    dp.produit_key,
    sr.composant_cible,
    sr.motif,
    dt.temps_key,
    sr.quantite,
    sr.date_saisie
from "airflow"."gold_staging"."stg_rebuts" sr
left join "airflow"."gold_intermediate"."int_production_cadence" ipc
    on ipc.code_of = sr.code_of
left join "airflow"."gold"."dim_machine" dm
    on dm.machine = ipc.machine
left join "airflow"."gold"."dim_produit" dp
    on dp.code = ipc.produit_code
left join "airflow"."gold"."dim_temps" dt
    on dt.date = date_trunc('day', sr.date_saisie)::date


  );
  
  
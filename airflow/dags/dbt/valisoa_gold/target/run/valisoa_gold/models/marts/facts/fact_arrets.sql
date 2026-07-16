
      
  
    

  create  table "airflow"."gold_gold"."fact_arrets__dbt_tmp"
  
  
    as
  
  (
    

select
    
    md5(
        
            coalesce(cast(sa.source_id as text), '')
    )
         as arret_key,
    sa.code_of,
    dm.machine_key,
    dma.motif_key,
    dt.temps_key,
    sa.date_debut,
    sa.date_fin,
    sa.duree_min
from "airflow"."gold_gold_staging"."stg_arrets" sa
left join "airflow"."gold_gold"."dim_machine" dm
    on dm.machine = sa.machine
left join "airflow"."gold_gold"."dim_motif_arret" dma
    on dma.type_arret = coalesce(sa.type_arret, 'NON_RENSEIGNE')
   and dma.motif       = coalesce(sa.motif, 'NON_RENSEIGNE')
left join "airflow"."gold_gold"."dim_temps" dt
    on dt.date = date_trunc('day', sa.date_debut)::date


  );
  
  
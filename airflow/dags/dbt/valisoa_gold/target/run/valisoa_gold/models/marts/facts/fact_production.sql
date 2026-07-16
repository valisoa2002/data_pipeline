
      
  
    

  create  table "airflow"."gold_gold"."fact_production__dbt_tmp"
  
  
    as
  
  (
    

-- Grain : un OF (code_of). Jointe aux dimensions machine, produit, temps.
-- Jointure produit sur le CODE (dp.code), pas le libellé, car dim_produit
-- utilise désormais le code comme clé métier stable.

select
    
    md5(
        
            coalesce(cast(ipc.code_of as text), '')
    )
          as production_key,
    ipc.code_of,
    dm.machine_key,
    dp.produit_key,
    dt.temps_key,
    ipc.date_debut,
    ipc.date_fin,
    ipc.qte_prevue,
    ipc.qte_produite,
    ipc.qte_rebut,
    ipc.trs_pct,
    ipc.disponibilite_pct,
    ipc.performance_pct,
    ipc.qualite_pct
from "airflow"."gold_gold_intermediate"."int_production_cadence" ipc
left join "airflow"."gold_gold"."dim_machine" dm
    on dm.machine = ipc.machine
left join "airflow"."gold_gold"."dim_produit" dp
    on dp.code = ipc.produit_code
left join "airflow"."gold_gold"."dim_temps" dt
    on dt.date = date_trunc('day', ipc.date_debut)::date


  );
  
  
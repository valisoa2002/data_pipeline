
  
    

  create  table "airflow"."gold_gold_ml"."production_features__dbt_tmp"
  
  
    as
  
  (
    -- Une ligne par OF, toutes les features à plat : contexte machine/produit,
-- indicateurs TRS, cumul des arrêts et rebuts associés à cet OF.
-- Pas de FK ici volontairement : consommé tel quel par des pipelines
-- ML (pandas/scikit-learn/notebooks), donc on dénormalise tout.

with arrets_agg as (
    select
        code_of,
        count(*)          as nb_arrets,
        sum(duree_min)     as duree_arrets_totale_min,
        sum(case when duree_min > 60 then 1 else 0 end) as nb_arrets_longs
    from "airflow"."gold_gold_staging"."stg_arrets"
    group by code_of
),

rebuts_agg as (
    select
        code_of,
        count(*)      as nb_lignes_rebut,
        sum(quantite)  as qte_rebut_declaree
    from "airflow"."gold_gold_staging"."stg_rebuts"
    group by code_of
)

select
    fp.production_key,
    fp.code_of,
    dm.machine,
    dm.atelier,
    dp.libelle          as produit,
    fp.date_debut,
    fp.date_fin,
    extract(epoch from (fp.date_fin - fp.date_debut)) / 60.0  as duree_of_min,
    fp.qte_prevue,
    fp.qte_produite,
    fp.qte_rebut,
    fp.trs_pct,
    fp.disponibilite_pct,
    fp.performance_pct,
    fp.qualite_pct,
    coalesce(aa.nb_arrets, 0)                as nb_arrets,
    coalesce(aa.duree_arrets_totale_min, 0)  as duree_arrets_totale_min,
    coalesce(aa.nb_arrets_longs, 0)          as nb_arrets_longs,
    coalesce(ra.nb_lignes_rebut, 0)          as nb_lignes_rebut,
    coalesce(ra.qte_rebut_declaree, 0)       as qte_rebut_declaree,
    -- cible potentielle pour un modèle de classification/régression :
    case when fp.qte_produite > 0
         then round(fp.qte_rebut::numeric / fp.qte_produite * 100, 2)
         else null
    end as taux_rebut_pct
from "airflow"."gold_gold"."fact_production" fp
left join "airflow"."gold_gold"."dim_machine" dm  on dm.machine_key = fp.machine_key
left join "airflow"."gold_gold"."dim_produit" dp  on dp.produit_key = fp.produit_key
left join arrets_agg aa               on aa.code_of = fp.code_of
left join rebuts_agg ra               on ra.code_of = fp.code_of
  );
  
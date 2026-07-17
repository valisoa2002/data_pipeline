-- Combine une ligne de réalisation avec ses indicateurs TRS correspondants
-- (grain : un OF). Si plusieurs lignes de cadence existent pour un même
-- OF (relevés successifs), on prend la plus récente (validated_at max).
--
-- NOTE : "produit_code" est extrait ici pour permettre la jointure avec
-- dim_produit (qui utilise le code comme clé stable) dans fact_production,
-- sans dupliquer cette logique de parsing dans plusieurs modèles.

with cadence_ranked as (
    select
        *,
        row_number() over (
            partition by code_of
            order by validated_at desc
        ) as rn
    from "airflow"."gold_staging"."stg_cadences"
),

cadence_latest as (
    select * from cadence_ranked where rn = 1
)

select
    r.code_of,
    r.atelier,
    r.machine,
    r.produit,
    trim(substring(r.produit from '\[([^\]]+)\]'))  as produit_code,
    r.date_debut,
    r.date_fin,
    r.qte_prevue,
    r.qte_produite,
    r.qte_rebut,
    r.statut,
    c.trs_pct,
    c.disponibilite_pct,
    c.performance_pct,
    c.qualite_pct
from "airflow"."gold_staging"."stg_realisations" r
left join cadence_latest c
    on c.code_of = r.code_of
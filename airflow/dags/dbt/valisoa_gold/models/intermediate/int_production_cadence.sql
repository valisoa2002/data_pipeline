-- Combine une ligne de réalisation avec ses indicateurs TRS correspondants
-- (grain : un OF). Si plusieurs lignes de cadence existent pour un même
-- OF (relevés successifs), on prend la plus récente (validated_at max).

with cadence_ranked as (
    select
        *,
        row_number() over (
            partition by code_of
            order by validated_at desc
        ) as rn
    from {{ ref('stg_cadences') }}
),

cadence_latest as (
    select * from cadence_ranked where rn = 1
)

select
    r.code_of,
    r.atelier,
    r.machine,
    r.produit,
    r.date_debut,
    r.date_fin,
    r.qte_prevue,
    r.qte_produite,
    r.qte_rebut,
    c.trs_pct,
    c.disponibilite_pct,
    c.performance_pct,
    c.qualite_pct
from {{ ref('stg_realisations') }} r
left join cadence_latest c
    on c.code_of = r.code_of
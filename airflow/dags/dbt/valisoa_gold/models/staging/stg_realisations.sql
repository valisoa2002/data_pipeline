-- Nettoyage léger sur validated.raw_excel_realisations (Silver)
-- Une ligne = un OF, avec les quantités prévues/produites/rebutées
--
-- DÉDUPLICATION EN DEUX TEMPS, deux phénomènes distincts observés :
--   1. Doublons d'ingestion stricts (fichiers Excel archivés qui se
--      chevauchent) : mêmes valeurs réintroduites à l'identique.
--   2. Relevés successifs légitimes d'un même OF en cours d'exécution
--      (plusieurs snapshots avec qte_produite=0 jusqu'au relevé final
--      qui contient les quantités réelles et la date_fin la plus tardive).
--
-- On garde donc, par code_of, le relevé le plus avancé : celui avec la
-- date_fin la plus tardive (le relevé final = l'état le plus complet de
-- l'OF), et non le plus récent selon validated_at (qui est identique
-- pour tous les doublons d'ingestion et ne permet pas de les départager
-- correctement).

with realisations_ranked as (
    select
        _id,
        code_of,
        atelier,
        machine,
        produit,
        date_debut,
        date_fin,
        qte_prevue,
        qte_produite,
        qte_rebut,
        statut,
        quality_score,
        validated_at,
        row_number() over (
            partition by trim(code_of)
            order by date_fin desc nulls last, qte_produite desc, _id desc
        ) as rn
    from {{ source('validated', 'raw_excel_realisations') }}
    where code_of is not null and trim(code_of) != ''
)

select
    _id                         as source_id,
    trim(code_of)                as code_of,
    trim(atelier)                as atelier,
    trim(machine)                as machine,
    trim(produit)                as produit,
    date_debut,
    date_fin,
    qte_prevue,
    qte_produite,
    qte_rebut,
    trim(statut)                  as statut,
    quality_score,
    validated_at
from realisations_ranked
where rn = 1
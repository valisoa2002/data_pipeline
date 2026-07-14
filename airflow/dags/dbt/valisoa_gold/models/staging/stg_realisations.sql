-- Nettoyage léger sur validated.raw_excel_realisations (Silver)
-- Une ligne = un OF, avec les quantités prévues/produites/rebutées

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
    quality_score,
    validated_at
from {{ source('validated', 'raw_excel_realisations') }}
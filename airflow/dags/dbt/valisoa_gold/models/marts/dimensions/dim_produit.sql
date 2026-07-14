-- NOTE : le staging actuel n'expose que le libellé produit (colonne
-- "produit" dans raw_excel_realisations). Pas de code produit distinct,
-- ni famille/gamme. On matérialise ce qu'on a et on prévoit les colonnes
-- pour un futur référentiel produit (à joindre plus tard).

with produits as (
    select distinct produit
    from {{ ref('stg_realisations') }}
    where produit is not null and trim(produit) != ''
)

select
    {{ surrogate_key(['produit']) }}   as produit_key,
    produit                             as libelle,
    cast(null as text) as code,
    cast(null as text) as famille,
    cast(null as text) as gamme
from produits
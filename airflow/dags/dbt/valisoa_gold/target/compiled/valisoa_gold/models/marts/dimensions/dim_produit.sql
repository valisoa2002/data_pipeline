-- Le staging expose "produit" au format : "[CODE] - Libellé produit"
-- (ex: "[VR011] - IMPEC Gel Hydro-alcoolique vrac").
-- On extrait le code entre crochets comme clé métier stable, et on
-- nettoie le libellé en retirant le préfixe "[CODE] - ".
-- famille/gamme : toujours absents du staging actuel -> NULL, à
-- enrichir plus tard via un référentiel produit dédié si besoin.

with produits_raw as (
    select distinct produit
    from "airflow"."gold_staging"."stg_realisations"
    where produit is not null and trim(produit) != ''
),

produits_parsed as (
    select
        produit                                                as produit_brut,
        trim(substring(produit from '\[([^\]]+)\]'))            as code,
        trim(regexp_replace(produit, '^\[[^\]]+\]\s*-\s*', '')) as libelle
    from produits_raw
)

select
    
    md5(
        
            coalesce(cast(code as text), '')
    )
   as produit_key,
    code,
    libelle,
    cast(null as text) as famille,
    cast(null as text) as gamme
from produits_parsed
where code is not null   -- exclut les libellés qui ne suivent pas le format attendu
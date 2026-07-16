-- Une ligne par machine distincte, croisée depuis toutes les tables
-- staging qui référencent une machine (réalisations + arrêts).
-- capacite/ligne : non présents dans le staging actuel -> NULL,
-- à enrichir plus tard via un référentiel machine dédié si besoin.

with machines as (
    select distinct machine, atelier
    from "airflow"."gold_gold_staging"."stg_realisations"
    where machine is not null and trim(machine) != ''

    union

    select distinct machine, cast(null as text) as atelier
    from "airflow"."gold_gold_staging"."stg_arrets"
    where machine is not null and trim(machine) != ''
),

deduped as (
    select
        machine,
        max(atelier) as atelier   -- on garde l'atelier connu s'il existe
    from machines
    group by machine
)

select
    
    md5(
        
            coalesce(cast(machine as text), '')
    )
              as machine_key,
    machine,
    atelier,
    cast(null as text) as ligne,
    cast(null as numeric) as capacite
from deduped
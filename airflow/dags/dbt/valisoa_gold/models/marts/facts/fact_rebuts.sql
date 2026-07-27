{{
    config(
        unique_key='rebut_key',
        incremental_strategy='delete+insert'
    )
}}

-- Grain : une saisie de rebut. Jointe à machine/produit via code_of
-- (résolu depuis int_production_cadence, qui porte déjà machine et
-- produit_code pour l'OF correspondant), et à temps via date_saisie.

select
    {{ surrogate_key(['sr.source_id']) }}         as rebut_key,
    sr.code_of,
    dm.machine_key,
    dp.produit_key,
    sr.composant_cible,
    sr.motif,
    dt.temps_key,
    sr.quantite,
    sr.date_saisie
from {{ ref('stg_rebuts') }} sr
left join {{ ref('int_production_cadence') }} ipc
    on ipc.code_of = sr.code_of
left join {{ ref('dim_machine') }} dm
    on dm.machine = ipc.machine
left join {{ ref('dim_produit') }} dp
    on dp.code = ipc.produit_code
left join {{ ref('dim_temps') }} dt
    on dt.date = date_trunc('day', sr.date_saisie)::date

{% if is_incremental() %}
where sr.date_saisie >= (select coalesce(max(date_saisie), '1900-01-01') from {{ this }})
{% endif %} 
{{
    config(
        unique_key='production_key',
        incremental_strategy='delete+insert'
    )
}}

-- Grain : un OF (code_of). Jointe aux dimensions machine, produit, temps.

select
    {{ surrogate_key(['ipc.code_of']) }}          as production_key,
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
from {{ ref('int_production_cadence') }} ipc
left join {{ ref('dim_machine') }} dm
    on dm.machine = ipc.machine
left join {{ ref('dim_produit') }} dp
    on dp.libelle = ipc.produit
left join {{ ref('dim_temps') }} dt
    on dt.date = date_trunc('day', ipc.date_debut)::date

{% if is_incremental() %}
where ipc.date_debut >= (select coalesce(max(date_debut), '1900-01-01') from {{ this }})
{% endif %}
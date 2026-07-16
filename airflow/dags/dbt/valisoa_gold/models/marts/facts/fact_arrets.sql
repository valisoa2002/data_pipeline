{{
    config(
        unique_key='arret_key',
        incremental_strategy='delete+insert'
    )
}}

select
    {{ surrogate_key(['sa.source_id']) }}         as arret_key,
    sa.code_of,
    dm.machine_key,
    dma.motif_key,
    dt.temps_key,
    sa.date_debut,
    sa.date_fin,
    sa.duree_min
from {{ ref('stg_arrets') }} sa
left join {{ ref('dim_machine') }} dm
    on dm.machine = sa.machine
left join {{ ref('dim_motif_arret') }} dma
    on dma.type_arret = coalesce(sa.type_arret, 'NON_RENSEIGNE')
   and dma.motif       = coalesce(sa.motif, 'NON_RENSEIGNE')
left join {{ ref('dim_temps') }} dt
    on dt.date = date_trunc('day', sa.date_debut)::date

{% if is_incremental() %}
where sa.date_debut >= (select coalesce(max(date_debut), '1900-01-01') from {{ this }})
{% endif %}
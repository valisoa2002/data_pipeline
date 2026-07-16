{{
    config(
        unique_key='rebut_key',
        incremental_strategy='delete+insert'
    )
}}

select
    {{ surrogate_key(['sr.source_id']) }}         as rebut_key,
    sr.code_of,
    sr.composant_cible,
    sr.motif,
    dt.temps_key,
    sr.quantite,
    sr.date_saisie
from {{ ref('stg_rebuts') }} sr
left join {{ ref('dim_temps') }} dt
    on dt.date = date_trunc('day', sr.date_saisie)::date

{% if is_incremental() %}
where sr.date_saisie >= (select coalesce(max(date_saisie), '1900-01-01') from {{ this }})
{% endif %}
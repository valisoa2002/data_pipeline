select
    _id                         as source_id,
    trim(code_of)                as code_of,
    qte_produite,
    trs_pct,
    disponibilite_pct,
    performance_pct,
    qualite_pct,
    validated_at
from "airflow"."validated"."raw_excel_cadences"
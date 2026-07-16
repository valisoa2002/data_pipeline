
    
    

select
    _id as unique_field,
    count(*) as n_records

from "airflow"."validated"."raw_excel_realisations"
where _id is not null
group by _id
having count(*) > 1



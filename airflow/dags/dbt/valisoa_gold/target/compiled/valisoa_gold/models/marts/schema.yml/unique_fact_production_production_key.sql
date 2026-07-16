
    
    

select
    production_key as unique_field,
    count(*) as n_records

from "airflow"."gold_gold"."fact_production"
where production_key is not null
group by production_key
having count(*) > 1



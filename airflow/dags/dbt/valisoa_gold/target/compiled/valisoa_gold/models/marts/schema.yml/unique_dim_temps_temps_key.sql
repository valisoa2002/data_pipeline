
    
    

select
    temps_key as unique_field,
    count(*) as n_records

from "airflow"."gold_gold"."dim_temps"
where temps_key is not null
group by temps_key
having count(*) > 1




    
    

select
    machine_key as unique_field,
    count(*) as n_records

from "airflow"."gold_gold"."dim_machine"
where machine_key is not null
group by machine_key
having count(*) > 1



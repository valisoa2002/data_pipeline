
    
    

select
    rebut_key as unique_field,
    count(*) as n_records

from "airflow"."gold_gold"."fact_rebuts"
where rebut_key is not null
group by rebut_key
having count(*) > 1




    
    

select
    arret_key as unique_field,
    count(*) as n_records

from "airflow"."gold"."fact_arrets"
where arret_key is not null
group by arret_key
having count(*) > 1



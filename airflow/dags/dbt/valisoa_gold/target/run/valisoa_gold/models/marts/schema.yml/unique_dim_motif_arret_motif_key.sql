
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    motif_key as unique_field,
    count(*) as n_records

from "airflow"."gold_gold"."dim_motif_arret"
where motif_key is not null
group by motif_key
having count(*) > 1



  
  
      
    ) dbt_internal_test
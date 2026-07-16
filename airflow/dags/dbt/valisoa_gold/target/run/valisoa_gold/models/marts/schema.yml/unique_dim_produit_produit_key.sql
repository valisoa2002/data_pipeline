
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    produit_key as unique_field,
    count(*) as n_records

from "airflow"."gold_gold"."dim_produit"
where produit_key is not null
group by produit_key
having count(*) > 1



  
  
      
    ) dbt_internal_test
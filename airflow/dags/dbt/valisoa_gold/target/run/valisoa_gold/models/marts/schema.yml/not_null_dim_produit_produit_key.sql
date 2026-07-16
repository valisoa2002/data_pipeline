
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select produit_key
from "airflow"."gold_gold"."dim_produit"
where produit_key is null



  
  
      
    ) dbt_internal_test
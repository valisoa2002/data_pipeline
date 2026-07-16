
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select production_key
from "airflow"."gold_gold"."fact_production"
where production_key is null



  
  
      
    ) dbt_internal_test
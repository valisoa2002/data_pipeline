
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select code_of
from "airflow"."gold"."fact_production"
where code_of is null



  
  
      
    ) dbt_internal_test

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select machine_key
from "airflow"."gold_gold"."dim_machine"
where machine_key is null



  
  
      
    ) dbt_internal_test
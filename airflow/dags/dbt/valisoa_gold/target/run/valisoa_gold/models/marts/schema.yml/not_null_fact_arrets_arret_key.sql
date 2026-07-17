
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select arret_key
from "airflow"."gold"."fact_arrets"
where arret_key is null



  
  
      
    ) dbt_internal_test
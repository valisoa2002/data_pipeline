
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _id
from "airflow"."validated"."raw_excel_realisations"
where _id is null



  
  
      
    ) dbt_internal_test
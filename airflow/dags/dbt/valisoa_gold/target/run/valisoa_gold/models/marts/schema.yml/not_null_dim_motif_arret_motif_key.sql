
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select motif_key
from "airflow"."gold"."dim_motif_arret"
where motif_key is null



  
  
      
    ) dbt_internal_test
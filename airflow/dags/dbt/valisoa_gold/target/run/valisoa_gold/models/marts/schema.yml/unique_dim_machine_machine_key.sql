
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    machine_key as unique_field,
    count(*) as n_records

from "airflow"."gold"."dim_machine"
where machine_key is not null
group by machine_key
having count(*) > 1



  
  
      
    ) dbt_internal_test
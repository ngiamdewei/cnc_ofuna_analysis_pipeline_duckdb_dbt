
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    production_cycle_id as unique_field,
    count(*) as n_records

from "cnc_ofuna_dev"."dbt_dev"."int_ofuna_production_cycles"
where production_cycle_id is not null
group by production_cycle_id
having count(*) > 1



  
  
      
    ) dbt_internal_test
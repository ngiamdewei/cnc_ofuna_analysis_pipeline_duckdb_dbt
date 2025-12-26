
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        machine_no as value_field,
        count(*) as n_records

    from "cnc_ofuna_dev"."raw"."stage_raw_cnc_ofuna"
    group by machine_no

)

select *
from all_values
where value_field not in (
    '084','085','086','087','088','089','090','091'
)



  
  
      
    ) dbt_internal_test
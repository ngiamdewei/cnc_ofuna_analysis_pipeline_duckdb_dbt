
    
    

with all_values as (

    select
        monitor_machineno as value_field,
        count(*) as n_records

    from "cnc_ofuna_dev"."dbt_dev"."int_ofuna_production_cycles"
    group by monitor_machineno

)

select *
from all_values
where value_field not in (
    84,85,86,87,88,89,90,91
)



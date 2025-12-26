
    
    

with all_values as (

    select
        monitor_machineno as value_field,
        count(*) as n_records

    from "cnc_ofuna_dev"."dbt_dev"."mart_ofuna_alarm"
    group by monitor_machineno

)

select *
from all_values
where value_field not in (
    '084','085','086','087','088','089','090','091'
)



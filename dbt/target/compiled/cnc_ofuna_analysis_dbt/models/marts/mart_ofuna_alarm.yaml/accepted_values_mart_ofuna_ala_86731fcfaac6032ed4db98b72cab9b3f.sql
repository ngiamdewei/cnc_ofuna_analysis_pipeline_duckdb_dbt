
    
    

with all_values as (

    select
        machine_Code as value_field,
        count(*) as n_records

    from "cnc_ofuna_dev"."raw"."mart_ofuna_alarm"
    group by machine_Code

)

select *
from all_values
where value_field not in (
    '084','085','086','087','088','089','090','091'
)



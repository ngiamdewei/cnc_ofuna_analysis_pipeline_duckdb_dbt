
  
    

  create  table "cnc_ofuna_dev"."raw"."int_ofuna_tools_nu_deltas__dbt_tmp"
  
  
    as
  
  (
    with base as (
    select
        monitor_machineno,
        "TimeStamp_SGT" as ts_start,
        "TimeStamp_UTC" as timestamp_utc,
        ("TimeStamp_SGT"::date) as production_day,
        monitor_nu as nu
    from "cnc_ofuna_dev"."raw"."stage_raw_cnc_ofuna"
    where monitor_nu is not null
),

nonzero as (
    select
        monitor_machineno,
        production_day,
        ts_start,
        timestamp_utc,
        nu,
        lag(nu) over (
            partition by monitor_machineno
            order by timestamp_utc
        ) as prev_nu
    from base
    where nu > 0
),

deltas as (
    select
        monitor_machineno,
        production_day,
        ts_start,
        timestamp_utc,
        lead(ts_start) over (
            partition by monitor_machineno
            order by timestamp_utc
        ) as ts_end,
        nu,
        prev_nu,
        case
            when prev_nu is null then 0
            when nu <= prev_nu then 0
            else nu - prev_nu
        end as nu_delta
    from nonzero
)

select
    monitor_machineno,
    production_day,
    ts_start,
    ts_end,
    nu,
    prev_nu,
    nu_delta
from deltas
  );
  
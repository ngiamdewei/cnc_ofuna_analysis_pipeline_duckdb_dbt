with cycles as (
    select
        monitor_machineno,
        production_cycle_id,
        production_day,
        monitor_program,
        cycle_start_sgt,
        cycle_end_sgt
    from {{ ref('int_ofuna_production_cycles') }}
),

cycle_nu as (
    select
        c.monitor_machineno,
        c.production_cycle_id,
        c.production_day,
        c.monitor_program,
        c.cycle_start_sgt,
        c.cycle_end_sgt,
        coalesce(sum(nu.nu_delta), 0) as production_hitscount
    from cycles c
    left join {{ ref('int_ofuna_tools_nu_deltas') }} nu
      on nu.monitor_machineno = c.monitor_machineno
     and nu.ts_start >= c.cycle_start_sgt
     and (c.cycle_end_sgt is null or nu.ts_start < c.cycle_end_sgt)
    group by
        c.monitor_machineno,
        c.production_cycle_id,
        c.production_day,
        c.monitor_program,
        c.cycle_start_sgt,
        c.cycle_end_sgt
),

cycle_alarms as (
    select
        c.monitor_machineno,
        c.production_cycle_id,
        coalesce(sum(a.alarm_waitingtime), 0)  as production_waitingtime,
        coalesce(sum(a.alarm_stoppingtime), 0) as production_stoppingtime,
        coalesce(count(*), 0)                  as production_alarmcount
    from cycles c
    left join {{ ref('mart_ofuna_alarm') }} a
      on a.machine_code = c.monitor_machineno
     and a.alarm_start >= c.cycle_start_sgt
     and (c.cycle_end_sgt is null or a.alarm_end <= c.cycle_end_sgt)
    group by
        c.monitor_machineno,
        c.production_cycle_id
)

select
    n.production_day::date                                   as production_day,
    n.monitor_machineno::varchar(20)                          as production_machineno,
    n.monitor_program::varchar(50)                            as production_progname,
    n.cycle_start_sgt::timestamp                              as production_starttime,
    n.cycle_end_sgt::timestamp                                as production_endtime,
    n.production_hitscount::int                               as production_hitscount,
    case
        when n.cycle_end_sgt is null then null
        else (
            extract(epoch from (n.cycle_end_sgt - n.cycle_start_sgt))::int
            - coalesce(a.production_waitingtime, 0)
            - coalesce(a.production_stoppingtime, 0)
        )
    end                                                       as production_processtime,
    coalesce(a.production_waitingtime, 0)::int                as production_waitiingtime,
    coalesce(a.production_stoppingtime, 0)::int               as production_stoppingtime,
    coalesce(a.production_alarmcount, 0)::int                 as production_alarmcount,
    (n.cycle_end_sgt is not null)                             as production_hitscountfinish,
    (n.cycle_end_sgt is not null)                             as production_endtimefinish,
    n.cycle_end_sgt::timestamp                                as production_timelog
from cycle_nu n
left join cycle_alarms a
  on a.monitor_machineno = n.monitor_machineno
 and a.production_cycle_id = n.production_cycle_id

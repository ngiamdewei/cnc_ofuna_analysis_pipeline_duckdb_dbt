with recent as (
    select
        monitor_machineno,
        timestamp_utc,
        timestamp_sgt,
        opctag,
        monitor_elapsed,
        monitor_progress,
        monitor_phdt,
        monitor_diameter,
        monitor_spindle,
        machine_state_ofuna,
        monitor_program,
        alarm_alarmcode,
        StatusErrorCode,
        machinestate
    from {{ ref('stage_raw_cnc_ofuna') }}
),


machines as (
    select lpad(gs::text, 3, '0')::varchar(20) as monitor_machineno
    from generate_series(84, 91) gs
),

latest_status as (
    select *
    from (
        select r.*,
               row_number() over (partition by r.monitor_machineno order by r.timestamp_sgt desc) as rn
        from recent r
        where r.opctag = 'Status'
    ) x
    where rn = 1
),

latest_error as (
    select *
    from (
        select 
         e.*,
         e.timestamp_sgt as alarm_start_sgt,
         row_number() over (partition by e.monitor_machineno order by e.timestamp_sgt desc) as rn
        from recent e
        join latest_status s
          on s.monitor_machineno = e.monitor_machineno
         and e.opctag = 'Error'
         and e.timestamp_sgt <= s.timestamp_sgt
        where e.alarm_alarmcode is not null
    ) x
    where rn = 1
),

latest_diameter as (
    select monitor_machineno, monitor_diameter
    from (
        select d.*,
               row_number() over (partition by d.monitor_machineno order by d.timestamp_sgt desc) as rn
        from recent d
        where d.opctag='Diameter'
          and d.monitor_diameter is not null
          and d.monitor_diameter > 0
    ) x
    where rn=1
),

joined as (
    select
        m.monitor_machineno,

        s.timestamp_sgt,
        s.monitor_elapsed,
        s.monitor_progress,
        s.monitor_phdt,
        coalesce(d.monitor_diameter, s.monitor_diameter) as monitor_diameter,
        s.monitor_spindle,
        s.monitor_program,
        s.machine_state_ofuna,
        s.StatusErrorCode,
        s.machinestate,
        e.alarm_alarmcode,
        e.alarm_start_sgt
    from machines m
    left join latest_status s on s.monitor_machineno = m.monitor_machineno
    left join latest_error  e on e.monitor_machineno = m.monitor_machineno
    left join latest_diameter d on d.monitor_machineno = m.monitor_machineno
),

status_calc as (
    select
        j.*,
        case
            when j.monitor_machineno is null then null
            when j.machine_state_ofuna = 'MOWORK' and j.monitor_elapsed > 0 then 2
            when j.alarm_alarmcode is not null and j.machine_state_ofuna in ('MOSTOP','MOALAM') then 1
            when j.alarm_alarmcode is not null and j.machine_state_ofuna in ('MOWAIT','MOIDLE') then 6
            when j.monitor_progress between 1 and 99 then 2
            when j.monitor_progress >= 100 then 2
            when j.monitor_progress is null and j.monitor_elapsed > 0 then 2
            when j.monitor_progress = 0 and j.monitor_elapsed > 0 then 2
            else null
        end as monitor_status,
        case
          when j.timestamp_sgt is null or j.monitor_elapsed is null then null
          else (j.timestamp_sgt - (j.monitor_elapsed * interval '1 second'))
        end as monitor_start
    from joined j
)


select
    -- =========================================================
    -- 18 “Power BI / Viamech base” columns FIRST (name + type)
    -- =========================================================
    monitor_machineno::varchar(20)                 as monitor_machineno,
    timestamp_sgt::timestamp                       as monitor_timelog,
    null::varchar(256)                             as monitor_type,
    monitor_status::int                            as monitor_status,
    monitor_spindle::varchar(6)                    as monitor_spindle,
    null::int                                      as monitor_sp,
    null::int                                      as monitor_mode,
    monitor_program::varchar(256)                  as monitor_program,
    monitor_start                                  as monitor_start,
    monitor_elapsed::int                           as monitor_elapsed,
    monitor_progress::int                          as monitor_progress,
    monitor_phdt::int                              as monitor_phdt,
    monitor_diameter::real                         as monitor_diameter,
    case
      when monitor_phdt is null or monitor_progress is null or monitor_progress <= 0 then null
      else floor(monitor_phdt * (100.0 - monitor_progress) / monitor_progress)::int
    end                                            as monitor_remain,
    alarm_alarmcode::varchar(6)                    as monitor_alarmcode,
    null::int                                      as monitor_ps,
    null::int                                      as monitor_sr,
    null::int                                      as monitor_pr,
    -- =========================================================
    -- EXTRA columns AFTER base (safe to keep)
    -- =========================================================
    CASE
        WHEN monitor_elapsed IS NULL THEN NULL
        ELSE TO_CHAR(INTERVAL '1 second' * monitor_elapsed,'HH24:MI:SS')
    END as elapsed_time,
    (
        CASE
            WHEN monitor_progress IS NULL or monitor_progress = 0 THEN NULL
            ELSE 100 * monitor_elapsed / monitor_progress
        END
    ) as total_time
    ,
    (
        CASE
            WHEN monitor_progress IS NULL  or monitor_progress = 0 THEN NULL
            ELSE (100 * monitor_elapsed / monitor_progress - monitor_elapsed)
        END
    ) as remaining_seconds
    ,
    (
        CASE
            WHEN monitor_progress IS NULL OR  monitor_progress = 0 THEN NULL
            ELSE TO_CHAR(INTERVAL '1 second' * (100 * monitor_elapsed / monitor_progress - monitor_elapsed),'HH24:MI:SS')
        END
    ) as remaining_time
    ,
    CASE
        WHEN alarm_alarmcode IS NULL OR alarm_start_sgt IS NULL THEN NULL
        ELSE (CURRENT_TIMESTAMP(0) - alarm_start_sgt)
    END as alarm_elapsed_time,
    CASE
        WHEN monitor_progress IS NULL OR monitor_progress < 100
        OR monitor_elapsed IS NULL OR monitor_start IS NULL
        THEN NULL
        ELSE (monitor_start + (monitor_elapsed || ' seconds')::interval)
    END as end_time,
    -- (
    --     monitor_start + (monitor_elapsed || ' seconds')::interval
    -- ) as end_time
    CASE
        WHEN monitor_progress IS NULL OR monitor_progress < 100
        OR monitor_elapsed IS NULL OR monitor_start IS NULL
        THEN NULL
        ELSE (CURRENT_TIMESTAMP(0) - (monitor_start + (monitor_elapsed || ' seconds')::interval))
    END as elapsed_end_time,
    CASE
        WHEN monitor_progress IS NULL OR monitor_progress < 100
        OR monitor_elapsed IS NULL OR monitor_start IS NULL
        THEN NULL
        ELSE EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP(0) - (monitor_start + (monitor_elapsed || ' seconds')::interval)))
    END as elapsed_end_time_seconds,
    case monitor_status
      when 1 then 'Stopping'
      when 2 then 'Working'
      when 6 then 'Waiting'
    end                                            as monitor_status_desc,
    machinestate                                   as "Machine Status",
    StatusErrorCode                                as StatusErrorCode,
    machine_state_ofuna                            as machine_state_ofuna_dbg
from status_calc

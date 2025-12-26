with src as (
  select
    monitor_machineno,
    timestamp_sgt,
    timestamp_utc,
    machine_state_ofuna,
    monitor_program,
    alarm_alarmcode,
    alarm_content
  from "cnc_ofuna_dev"."raw"."stage_raw_cnc_ofuna"
),

state_ordered as (
  select
    s.*,
    lag(s.machine_state_ofuna) over (
      partition by s.monitor_machineno
      order by s.timestamp_sgt
    ) as prev_state
  from src s
  where s.machine_state_ofuna is not null    
),


segments as (
  select
    monitor_machineno,
    timestamp_sgt as segment_start_sgt,
    lead(timestamp_sgt) over (partition by monitor_machineno order by timestamp_sgt) as segment_end_sgt,
    machine_state_ofuna,
    prev_state,
    monitor_program,
    timestamp_utc
  from state_ordered
  where machine_state_ofuna in ('MOWAIT','MOIDLE','MOSTOP','MOALAM')
    and (prev_state is null or prev_state <> machine_state_ofuna)
),

seg_alarm as (
  select
    sg.*,
    al.alarm_alarmcode as seg_alarmcode,
    al.alarm_content   as seg_alarmcontent
  from segments sg
  left join lateral (
    select a.alarm_alarmcode, a.alarm_content
    from src a
    where a.monitor_machineno = sg.monitor_machineno
      and a.alarm_alarmcode is not null
      and a.timestamp_sgt <= sg.segment_start_sgt
    order by a.timestamp_utc desc
    limit 1
  ) al on true
),

dur as (
  select
    *,
    case when segment_end_sgt is null then null
         else extract(epoch from (segment_end_sgt - segment_start_sgt))::int
    end as duration_s
  from seg_alarm
)

select
  -- =========================================================
  -- 10 base columns FIRST (so downstream stays stable)
  -- NOTE: alarm_continue is varchar(1) so Power BI filter = 'T' works
  -- =========================================================
  segment_start_sgt::date               as alarm_Day,
  monitor_machineno::varchar(20)        as machine_Code,
  seg_alarmcode::varchar(6)             as alarm_Code,
  seg_alarmcontent                      as alarm_Msg,
  null as Alarm_Level,
  segment_start_sgt::timestamp          as alarm_Start,
  segment_end_sgt::timestamp            as alarm_End,
  0::int                                as alarm_workingtime,
  case when machine_state_ofuna in ('MOWAIT','MOIDLE')
       then coalesce(duration_s,0) else 0 end::int
                                       as alarm_waitingtime,
  case when machine_state_ofuna in ('MOSTOP','MOALAM')
       then coalesce(duration_s,0) else 0 end::int
                                       as alarm_stoppingtime,
  case when machine_state_ofuna in ('MOSTOP','MOALAM') then 1
       when machine_state_ofuna in ('MOWAIT','MOIDLE') then 6
       else null end::int                as alarm_machinestate,
  case when segment_end_sgt is null then 'T' else 'F' end::varchar(1)
                                       as alarm_continue,
  -- (Alarm_End - Alarm_Start) as "Duration(HH:mm:ss)",
  case
    when segment_end_sgt is null then null
    else (segment_end_sgt - segment_start_sgt)
  end                                   as "Duration(HH:mm:ss)",
  duration_s as "Duration(s)",
  -- =========================================================
  -- EXTRA columns AFTER base
  -- =========================================================
  monitor_program,
  prev_state,
  timestamp_utc                           as timestamp_utc_dbg,
  'NCdrill'::text                         as "Machine Type"

from dur
where seg_alarmcode is not null
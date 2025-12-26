with base as (
    select
        machine_no,
        timestamp_utc,
        timestamp_sgt,
        status as quality,
        value,
        pointname,
        regexp_replace(pointname, '^.*\.', '') as opctag
    from {{ source('public', 'raw_cnc_ofuna') }}
),

parsed as (
    select
        lpad(machine_no::text, 3, '0')                         as monitor_machineno,
        timestamp_sgt                                          as timestamp_sgt,
        timestamp_utc                                          as timestamp_utc,
        quality                                                as quality,
        value                                                  as value,
        pointname                                              as pointname,
        opctag                                                 as opctag,

        /* Diameter */
        case
            when opctag = 'Diameter'
            then nullif(value, '')::float / 1000.0
        end                                                    as monitor_diameter,

        /* monitor_elapsed from Status tag: ARhh:mm:ss -> seconds */
        case
            when opctag = 'Status' and value ~ 'AR[0-9]{2}:[0-9]{2}:[0-9]{2}'
            then extract(epoch from ( (regexp_match(value, 'AR([0-9]{2}:[0-9]{2}:[0-9]{2})'))[1]::time ))::int
        end                                                    as monitor_elapsed,

        /* monitor_phdt from Status tag: AHxxxxxxx */
        case
            when opctag = 'Status' and value ~ 'AH'
            then nullif(btrim((regexp_match(value, 'AH\s*([0-9]{1,7})'))[1]), '')::int
        end                                                    as monitor_phdt,

        /* monitor_progress from Status tag: APxxx */
        case
            when opctag = 'Status' and value ~ 'AP'
            then nullif(btrim((regexp_match(value, 'AP\s*([0-9]{1,3})'))[1]), '')::int
        end                                                    as monitor_progress,

        /* ZSxxxxx raw + spindle bits + MOxxxxx */
        case
            when opctag = 'Status' and value ~ 'ZS'
            then (regexp_match(value, '(ZS[^,]*)'))[1]
        end                                                    as spindlefieldraw,

        case
            when opctag = 'Status' and value ~ 'ZS'
            then right((regexp_match(value, '(ZS[^,]*)'))[1], 6)
        end                                                    as monitor_spindle,

        case
            when opctag = 'Status' and value ~ ',MO'
            then (regexp_match(value, ',(MO[^,]*)'))[1]
        end                                                    as machine_state_ofuna,

        /* StatusErrorCode token like ",ECxxxx" */
        case
            when opctag = 'Status' and value ~ ',EC'
            then (regexp_match(value, ',(EC[^,]*)'))[1]
        end                                                    as statuserrorcode,

        /* monitor_program token ",FNxxxxx..." (keep rest of string like MSSQL SUBSTRING(...,255)) */
        case
            when opctag = 'Status' and value ~ ',FN'
            then btrim(substring(value from (position(',FN' in value) + 3)))
        end                                                    as monitor_program,

        /* machinestate from MachineState tag */
        case
            when opctag = 'MachineState'
            then nullif(value, '')::int
        end                                                    as machinestate,

        /* Tools tag: tool number */
        case
            when opctag = 'Tools' and value ~ 'T[0-9]+'
            then (regexp_match(value, 'T([0-9]+)'))[1]::int
        end                                                    as monitor_toolno,

        /* Tools tag: NU cumulative hits */
        case
            when opctag = 'Tools' and value ~ 'NU[0-9]+'
            then (regexp_match(value, 'NU([0-9]+)'))[1]::int
        end                                                    as monitor_nu,

        /* Error tag alarm parsing (mirror your OUTER APPLY logic) */
        case
            when opctag = 'Error' and position('*' in value) > 0
            then
                case
                    when value ~ '^[0-3][0-9]/[01][0-9]/[0-9]{2} [0-2][0-9]:[0-5][0-9]:[0-5][0-9] '
                    then btrim(substring(value from 19 for (position('*' in value) - 21)))
                    else btrim(left(value, position('*' in value) - 1))
                end
        end                                                    as error_monitor_program,

        case
            when opctag = 'Error' and position('*' in value) > 0 and position(';' in value) > 0
            then
                btrim(
                    substring(
                        value
                        from (case when position(' * ' in value) > 0 then position(' * ' in value) + 2 else position('*' in value) end) + 1
                        for (length(value) - position(';' in reverse(value)) + 1) - ((case when position(' * ' in value) > 0 then position(' * ' in value) + 2 else position('*' in value) end) + 1)
                    )
                )
        end                                                    as alarm_content,

        case
            when opctag = 'Error'
            then nullif((regexp_match(value, ';\s*([0-9]+)\s*$'))[1], '')::int
        end                                                    as alarm_alarmcode

    from base
),

final as (
    select
        monitor_machineno,
        timestamp_sgt as "TimeStamp_SGT",
        timestamp_utc as "TimeStamp_UTC",
        quality,
        value,
        pointname,
        opctag as "OpcTag",

        monitor_diameter,
        monitor_elapsed,
        monitor_phdt,
        monitor_progress,

        spindlefieldraw as "SpindleFieldRaw",
        monitor_spindle,
        machine_state_ofuna,

        alarm_content,
        alarm_alarmcode,

        statuserrorcode as "StatusErrorCode",

        /* prefer Status program, fallback to Error-derived program if you want */
        coalesce(monitor_program, error_monitor_program) as monitor_program,

        machinestate,
        monitor_toolno,
        monitor_nu
    from parsed
)

select * from final;

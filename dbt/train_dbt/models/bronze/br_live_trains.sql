{{
    config(
        materialized='table'
    )
}}

with raw_payload as (
    select *
    from read_json_auto('../data/staging/live_trains.json', sample_size=10000)
),
flattened as (
    select
        trainNumber as train_number,
        departureDate as departure_date,
        operatorShortCode as operator_short_code,
        trainType as train_type,
        cancelled,
        version,
        timestamp,
        timetableRow.stationshortcode as station_short_code,
        timetableRow.type as event_type,
        timetableRow.scheduledTime as scheduled_time,
        timetableRow.actualTime as actual_time,
        timetableRow.differenceInMinutes as difference_in_minutes
    from raw_payloadcross join unnest(timeTableRows) as timetableRow
    -- Virheellinen rivi (esimerkki): trainTypee as train_type
)
select *
from flattened
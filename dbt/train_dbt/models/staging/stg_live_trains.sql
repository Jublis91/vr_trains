{{ config(materialized='view') }}

with trains as (
  select *
  from {{ source('digitraffic', 'live_trains') }}
),

events as (
  select
    trainNumber::integer as train_number,
    departureDate::date as departure_date,
    operatorShortCode::varchar as operator_short_code,
    trainType::varchar as train_type,
    cancelled::boolean as cancelled,
    version::integer as version,
    timetableAcceptanceDate::timestamp as api_timestamp,
    unnest(timeTableRows) as ttr
  from trains
),

final as (
  select
    {{ dbt_utils.generate_surrogate_key([
      "cast(train_number as varchar)",
      "cast(departure_date as varchar)",
      "cast(ttr.stationShortCode as varchar)",
      "cast(ttr.scheduledTime as varchar)",
      "cast(ttr.type as varchar)"
    ]) }} as train_event_id,

    train_number,
    departure_date,
    operator_short_code,
    train_type,
    cancelled,
    version,
    api_timestamp,

    ttr.stationShortCode::varchar as station_short_code,
    ttr.type::varchar as event_type,
    ttr.scheduledTime::timestamp as scheduled_time,
    ttr.actualTime::timestamp as actual_time,
    ttr.differenceInMinutes::integer as difference_in_minutes
  from events
)

select * from final

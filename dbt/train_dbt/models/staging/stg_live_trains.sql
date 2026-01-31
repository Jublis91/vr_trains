with source_data as (
    select *
    from {{ source('digitraffic', 'live_trains') }}
),
renamed as (
    select
    {{ dbt_utils.surrogate_key(['train_number', 'departure_date', 'station_short_code', 'scheduled_time']) }} as train_event_id,
    train_number,
    departure_date,
    operator_short_code,
    train_type,
    cancelled,
    version,
    timestamp as api_timestamp,
    station_short_code,
    event_type,
    scheduled_time,
    actual_time,
    difference_in_minutes
from source_data
)
select *
from renamed
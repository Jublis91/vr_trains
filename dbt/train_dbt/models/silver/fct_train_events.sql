{{
    config(materialized='table')
}}

select
    train_event_id,
    train_number,
    departure_date,
    station_short_code as station_id,
    event_type,
    scheduled_time,
    difference_in_minutes,
    cancelled
from {{ ref('stg_live_trains') }}
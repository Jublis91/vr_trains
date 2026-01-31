{{
    config(materialized='table')
}}

select
    station_id,
    date_trunc('day', scheduled_time) as scheduled_date,
    count(*) as event_count,
    avg(difference_in_minutes) as avg_delay_minutes
from {{ ref('fct_train_events') }}
group by 1, 2
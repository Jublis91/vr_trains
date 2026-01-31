{{
    config(materialized='table')
}}

select distinct
    station_short_code as station_id
from {{ ref('stg_live_trains') }}
where station_short_code is not null
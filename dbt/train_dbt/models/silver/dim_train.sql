{{
    config(materialized='table')
}}

select distinct
    train_number,
    departure_date,
    operator_short_code,
    train_type
from {{ ref('stg_live_trains') }}
{{ config(enabled=false) }}

{{
    config(materialized='table')
}}

select *
from read_csv_auto('../../data/sample_train_events.csv', header=true)
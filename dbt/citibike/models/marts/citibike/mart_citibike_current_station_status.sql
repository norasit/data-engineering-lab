{{ config(
    materialized='view'
) }}

select *
from {{ ref('int_citibike_station_snapshot') }}
where snapshot_at_utc = (
    select max(snapshot_at_utc)
    from {{ ref('int_citibike_station_snapshot') }}
)
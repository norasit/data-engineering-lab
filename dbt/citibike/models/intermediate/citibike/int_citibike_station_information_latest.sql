{{ config(
    materialized='view'
) }}

select
    snapshot_at_utc,
    snapshot_at_ny,
    gbfs_version,
    station_id,
    name,
    short_name,
    lat,
    lon,
    region_id,
    capacity,
    has_kiosk

from {{ ref('stg_citibike_station_information') }}

qualify row_number() over (
    partition by station_id
    order by snapshot_at_utc desc
) = 1
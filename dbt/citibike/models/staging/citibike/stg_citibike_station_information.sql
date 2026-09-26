{{ config(
    materialized='view'
) }}

select
    -- Snapshot identity
    snapshot_at as snapshot_at_utc,
    datetime(snapshot_at, 'America/New_York') as snapshot_at_ny,

    -- Source contract
    version as gbfs_version,

    -- Station identity and descriptive metadata
    station_id,
    name,
    short_name,

    -- Location
    lat,
    lon,
    region_id,

    -- Station capacity
    capacity,

    -- Provider-specific attribute retained for analytics
    has_kiosk

from {{ source('citibike', 'station_information_snapshot') }}
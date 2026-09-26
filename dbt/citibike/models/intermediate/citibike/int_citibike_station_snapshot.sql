{{ config(
    materialized='view'
) }}

select
    -- Snapshot identity
    status.snapshot_at_utc,
    status.snapshot_at_ny,

    -- Source contract
    status.gbfs_version,

    -- Station identity
    status.station_id,

    -- Station metadata
    info.name,
    info.short_name,
    info.lat,
    info.lon,
    info.region_id,
    info.capacity,
    info.has_kiosk,

    -- Availability metrics
    status.num_bikes_available,
    status.num_bikes_disabled,
    status.num_docks_available,
    status.num_docks_disabled,
    status.num_ebikes_available,

    -- Operational flags
    status.is_installed,
    status.is_renting,
    status.is_returning,

    -- Source update timestamps
    status.last_updated_at_utc,
    status.last_updated_at_ny,
    status.last_reported_at_utc,
    status.last_reported_at_ny

from {{ ref('stg_citibike_station_status') }} as status

left join {{ ref('int_citibike_station_information_latest') }} as info
    on status.station_id = info.station_id
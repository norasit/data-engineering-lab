{{ config(
    materialized='view'
) }}

select
    -- Snapshot identity
    snapshot_at as snapshot_at_utc,
    datetime(snapshot_at, 'America/New_York') as snapshot_at_ny,

    -- Source contract
    version as gbfs_version,

    -- Station identity
    station_id,

    -- Availability metrics
    num_bikes_available,
    num_bikes_disabled,
    num_docks_available,
    num_docks_disabled,
    num_ebikes_available,

    -- Station operational flags
    is_installed = 1 as is_installed,
    is_renting = 1 as is_renting,
    is_returning = 1 as is_returning,

    -- Feed update time
    timestamp_seconds(last_updated) as last_updated_at_utc,
    datetime(
        timestamp_seconds(last_updated),
        'America/New_York'
    ) as last_updated_at_ny,

    -- Station report time
    timestamp_seconds(last_reported) as last_reported_at_utc,
    datetime(
        timestamp_seconds(last_reported),
        'America/New_York'
    ) as last_reported_at_ny

from {{ source('citibike', 'station_status_snapshot') }}
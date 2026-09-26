{{ config(
    materialized='view'
) }}

with daily_metrics as (

    select
        date(snapshot_at_ny) as date_ny,
        station_id,

        count(*) as snapshot_count,

        avg(num_bikes_available) as avg_bikes_available,
        min(num_bikes_available) as min_bikes_available,
        max(num_bikes_available) as max_bikes_available,

        avg(num_ebikes_available) as avg_ebikes_available,

        avg(num_docks_available) as avg_docks_available,
        min(num_docks_available) as min_docks_available,
        max(num_docks_available) as max_docks_available

    from {{ ref('int_citibike_station_snapshot') }}

    group by
        date_ny,
        station_id
),

latest_daily_metadata as (

    select
        date(snapshot_at_ny) as date_ny,
        station_id,
        name,
        short_name,
        lat,
        lon,
        region_id,
        capacity,
        has_kiosk

    from {{ ref('int_citibike_station_snapshot') }}

    qualify row_number() over (
        partition by date(snapshot_at_ny), station_id
        order by snapshot_at_ny desc
    ) = 1
)

select
    metrics.date_ny,
    metrics.station_id,

    metadata.name,
    metadata.short_name,
    metadata.lat,
    metadata.lon,
    metadata.region_id,
    metadata.capacity,
    metadata.has_kiosk,

    metrics.snapshot_count,
    metrics.avg_bikes_available,
    metrics.min_bikes_available,
    metrics.max_bikes_available,
    metrics.avg_ebikes_available,
    metrics.avg_docks_available,
    metrics.min_docks_available,
    metrics.max_docks_available

from daily_metrics as metrics
left join latest_daily_metadata as metadata
    on metrics.date_ny = metadata.date_ny
    and metrics.station_id = metadata.station_id
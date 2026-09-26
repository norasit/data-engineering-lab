# dbt Staging Design

## Purpose

This document records the approved design decisions for the dbt staging layer of the Citi Bike project.

The staging layer sits between the BigQuery ingestion snapshot tables and any future intermediate or mart models.

Its purpose is to:

- expose analytics-friendly columns
- apply lightweight semantic type conversions
- standardize timestamps and booleans
- remove ingestion-only or low-value technical fields
- preserve the source grain
- avoid aggregation and heavy business logic

Current materialization choice:

- staging models should be implemented as **BigQuery views**

This choice is appropriate for the current project because the staging logic is lightweight and the source data volume is small. Materialization can be reconsidered later if query cost, performance, or complexity increases.

---

# Model: `stg_citibike_station_status`

## Source

BigQuery source table:

`data-engineering-lab-nk.nyc_citibike.station_status_snapshot`

## Grain

`(snapshot_at_utc, station_id)`

The staging model must preserve the snapshot-level station grain.

No aggregation should be performed in this model.

---

## Timezone Policy

The ingestion layer keeps technical timestamps in their original source / ingestion representation.

The staging layer should expose both:

- UTC timestamps for unambiguous system-level comparison
- New York local datetimes for easier operational and analytical interpretation

New York timezone:

`America/New_York`

Do not use a fixed UTC offset because New York observes daylight saving time.

---

## Approved Column Mapping

| Source column | Staging column | Staging type | Transformation / Notes |
|---|---|---|---|
| `snapshot_at` | `snapshot_at_utc` | TIMESTAMP | Keep the absolute snapshot timestamp. |
| `snapshot_at` | `snapshot_at_ny` | DATETIME | Convert `snapshot_at` to New York local datetime using `America/New_York`. |
| `version` | `gbfs_version` | STRING | Rename for clarity. Represents the source GBFS version. |
| `station_id` | `station_id` | STRING | Keep unchanged. |
| `num_bikes_available` | `num_bikes_available` | INT64 | Keep unchanged. |
| `num_bikes_disabled` | `num_bikes_disabled` | INT64 | Keep unchanged. |
| `num_docks_available` | `num_docks_available` | INT64 | Keep unchanged. |
| `num_docks_disabled` | `num_docks_disabled` | INT64 | Keep unchanged. |
| `num_ebikes_available` | `num_ebikes_available` | INT64 | Keep. Observed to vary meaningfully across snapshots and stations. |
| `is_installed` | `is_installed` | BOOL | Convert GBFS v1.1 integer flag `0/1` to boolean. |
| `is_renting` | `is_renting` | BOOL | Convert GBFS v1.1 integer flag `0/1` to boolean. |
| `is_returning` | `is_returning` | BOOL | Convert GBFS v1.1 integer flag `0/1` to boolean. |
| `last_updated` | `last_updated_at_utc` | TIMESTAMP | Convert Unix epoch seconds to TIMESTAMP. |
| `last_updated` | `last_updated_at_ny` | DATETIME | Convert Unix epoch seconds to New York local datetime. |
| `last_reported` | `last_reported_at_utc` | TIMESTAMP | Convert Unix epoch seconds to TIMESTAMP. |
| `last_reported` | `last_reported_at_ny` | DATETIME | Convert Unix epoch seconds to New York local datetime. |

---

## Columns Intentionally Excluded from Staging

| Source column | Decision | Rationale |
|---|---|---|
| `snapshot_date` | Drop | Derived from `snapshot_at`; the ingestion value is based on Bangkok calendar date and is not needed in the analytical staging layer. |
| `ingested_at` | Drop | Ingestion/audit metadata. Available in the source snapshot table when operational debugging is required. |
| `source_object_uri` | Drop | Technical lineage metadata. Available in the ingestion table and raw S3 layer. |
| `ttl` | Drop | Feed operational metadata with no current analytical use case. |
| `legacy_id` | Drop | Provider-specific static identifier. Observed to remain stable per station and has no current analytical use case. |
| `eightd_has_available_keys` | Drop | Provider-specific extension. Observed without meaningful variation in the reviewed data. |
| `num_scooters_available` | Drop | Provider-specific metric. Observed as zero in the reviewed dataset and currently provides no analytical value. |
| `num_scooters_unavailable` | Drop | Provider-specific metric. Observed as zero in the reviewed dataset and currently provides no analytical value. |

---

## Provider Extension Decision Notes

The provider-specific station-status fields were reviewed using the data currently available in:

`data-engineering-lab-nk.nyc_citibike.station_status_snapshot`

The purpose of the review was to avoid keeping extension fields only because they exist in the source payload.

Current decisions:

- `num_ebikes_available` is retained because it shows meaningful variation across snapshots and stations and is analytically useful.
- `legacy_id` is excluded because it behaves as a static provider identifier rather than a time-series metric.
- `eightd_has_available_keys` is excluded because the reviewed data did not show meaningful analytical variation.
- `num_scooters_available` and `num_scooters_unavailable` are excluded because the reviewed values were consistently zero.

These decisions apply only to the dbt staging layer.

The original values remain preserved in:

- the BigQuery ingestion snapshot table
- the original raw JSON stored in S3

Therefore, excluded fields can be reconsidered later without losing source data.

---

## Staging Responsibilities

`stg_citibike_station_status` should:

- preserve one row per `(snapshot_at_utc, station_id)`
- perform semantic type cleanup
- expose both UTC and New York representations for relevant timestamps
- convert GBFS integer status flags to booleans
- expose metrics that are useful for downstream analytics
- remain simple and source-oriented

It should **not**:

- aggregate station metrics
- calculate daily summaries
- join to `station_information`
- introduce complex business rules
- perform current-state filtering
- contain mart-level logic

Those responsibilities belong in later intermediate or mart models.

---

## Future dbt Tests

When this model is implemented, consider at least:

- `station_id` not null
- `snapshot_at_utc` not null
- uniqueness of `(snapshot_at_utc, station_id)`
- accepted values for `gbfs_version` while the source remains on GBFS `1.1`
- non-negative checks for availability counts where appropriate

Exact dbt tests should be finalized together with the model implementation.

---

# Next Design Item

The next staging model to design is:

`stg_citibike_station_information`

Its schema should be reviewed independently using the same principles:

1. start from the actual BigQuery ingestion schema
2. distinguish analytical fields from ingestion metadata
3. apply only lightweight semantic transformations
4. review provider-specific extensions based on observed data and usefulness
5. preserve the source grain

---

# Model: `stg_citibike_station_information`

## Source

BigQuery source table:

`data-engineering-lab-nk.nyc_citibike.station_information_snapshot`

## Grain

`(snapshot_at_utc, station_id)`

The staging model must preserve one row per station per snapshot.

No aggregation should be performed in this model.

---

## Timezone Policy

Use the same timezone policy as `stg_citibike_station_status`.

Expose both:

- UTC timestamp for unambiguous system-level comparison
- New York local datetime for operational and analytical interpretation

New York timezone:

`America/New_York`

Do not use a fixed UTC offset because New York observes daylight saving time.

---

## Approved Column Mapping

| Source column | Staging column | Staging type | Transformation / Notes |
|---|---|---|---|
| `snapshot_at` | `snapshot_at_utc` | TIMESTAMP | Keep the absolute snapshot timestamp. |
| `snapshot_at` | `snapshot_at_ny` | DATETIME | Convert `snapshot_at` to New York local datetime using `America/New_York`. |
| `version` | `gbfs_version` | STRING | Rename for clarity. Represents the source GBFS version. |
| `station_id` | `station_id` | STRING | Keep unchanged. |
| `name` | `name` | STRING | Keep unchanged. |
| `short_name` | `short_name` | STRING | Keep unchanged. |
| `lat` | `lat` | FLOAT64 | Keep unchanged. |
| `lon` | `lon` | FLOAT64 | Keep unchanged. |
| `region_id` | `region_id` | STRING | Keep nullable. Missing values were observed in the ingestion data. |
| `capacity` | `capacity` | INT64 | Keep unchanged. Capacity varies across stations and can change between snapshots. |
| `has_kiosk` | `has_kiosk` | BOOL | Keep. Provider-specific extension with observed TRUE/FALSE variation. |

---

## Columns Intentionally Excluded from Staging

| Source column | Decision | Rationale |
|---|---|---|
| `snapshot_date` | Drop | Derived from `snapshot_at`; the ingestion value is based on Bangkok calendar date and is unnecessary in the analytical staging layer. |
| `ingested_at` | Drop | Ingestion/audit metadata. Available in the source snapshot table for operational debugging. |
| `source_object_uri` | Drop | Technical lineage metadata. Available in the ingestion table and raw S3 layer. |
| `ttl` | Drop | Feed operational metadata with no current analytical use case. |
| `rental_methods` | Drop | Operational capability metadata with no current analytical use case for this project. |
| `rental_uris` | Drop | Application/deep-link metadata intended for operational client usage rather than analytics. |
| `station_type` | Drop | Provider-specific extension. Reviewed data showed only one observed value (`classic`), so it currently adds no analytical distinction. |
| `external_id` | Drop | Provider-specific static identifier with no current analytical use case. |
| `electric_bike_surcharge_waiver` | Drop | Provider-specific extension. Reviewed data showed no meaningful variation. |
| `eightd_station_services` | Drop | Provider-specific extension already excluded from the BigQuery ingestion contract by human decision. The raw JSON remains preserved in S3. |
| `eightd_has_key_dispenser` | Drop | Provider-specific extension already excluded from the BigQuery ingestion contract by human decision. The raw JSON remains preserved in S3. |

---

## Observed Data Notes

The design was informed by the data currently available in:

`data-engineering-lab-nk.nyc_citibike.station_information_snapshot`

The reviewed data showed:

- most modeled fields were populated consistently
- `region_id` was missing in a small number of rows, so it remains nullable
- `capacity` showed meaningful variation across stations and some changes over time
- `has_kiosk` showed both TRUE and FALSE values and therefore remains useful as a station attribute
- `station_type` showed only the value `classic` in the reviewed data
- `external_id` behaved as a static provider identifier
- `electric_bike_surcharge_waiver` showed no meaningful variation in the reviewed data
- `rental_methods` and `rental_uris` were judged operational rather than analytically useful for the current project scope

These staging decisions do not remove data from the ingestion layer.

Excluded source fields remain available in the BigQuery ingestion snapshot table when present, and the original provider payload remains preserved in S3.

---

## Staging Responsibilities

`stg_citibike_station_information` should:

- preserve one row per `(snapshot_at_utc, station_id)`
- expose station identity and descriptive metadata useful for analytics
- expose station coordinates and capacity
- preserve nullable `region_id`
- expose both UTC and New York representations of the snapshot time
- remain simple and source-oriented

It should **not**:

- aggregate station data
- join to station-status snapshots
- calculate current-state records
- introduce mart-level business logic
- retain operational app/deep-link metadata solely because it exists in the source

Those responsibilities belong in later intermediate or mart models.

---

## Future dbt Tests

When this model is implemented, consider at least:

- `station_id` not null
- `snapshot_at_utc` not null
- uniqueness of `(snapshot_at_utc, station_id)`
- `name` not null
- accepted values for `gbfs_version` while the source remains on GBFS `1.1`
- non-negative `capacity`
- latitude between `-90` and `90`
- longitude between `-180` and `180`

Exact dbt tests should be finalized together with the model implementation.

---

# Staging Design Status

The staging schema design is now defined for:

- `stg_citibike_station_status`
- `stg_citibike_station_information`

The next step is to initialize / configure the dbt project and implement these models as BigQuery views.


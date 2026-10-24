# Taxi zone reference seed

Source: <https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv>

| Property | Value |
|---|---|
| Downloaded | 2026-10-24 |
| Size | 12,331 bytes |
| SHA-256 | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` |
| Data rows | 265 |

The CSV is committed because it is small, changes rarely, and is required to map
`PULocationID` / `DOLocationID` to borough and zone names. `dbt seed` loads it into
`delta.reference.taxi_zone_lookup`. Do not silently drop zones `264`, `265`, or
unmapped IDs; the models keep them explicit.

-- Example: materialize a local Iceberg table into a native ClickHouse table.
--
-- WARNING: the path in icebergLocal(...) is read by the CLICKHOUSE SERVER from
-- ITS OWN filesystem, NOT from the machine running this control panel. Use a
-- path that is valid on the ClickHouse host. This app does not validate it.
{{ config(materialized='table') }}

select *
from icebergLocal('/absolute/path/on/clickhouse/iceberg_output/oasis/product_base')

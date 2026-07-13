-- products_count: materialize a local Iceberg table into a native ClickHouse table.
--
-- WARNING: the icebergLocal(...) path is read by the CLICKHOUSE SERVER from its
-- own filesystem, not this host. Use a path valid on the ClickHouse host.
{{ config(materialized='table') }}

select branch_id, product_code,count() count from icebergLocal('/var/lib/clickhouse/user_files/iceberg_output/oasis/product_base')
group by all
{{ config(materialized='table', file_format='parquet') }}
SELECT
    cast(id AS bigint) AS id,
    customer,
    cast(amount AS bigint) AS amount,
    reflect('java.lang.System', 'getenv', 'HOSTNAME') AS executor_host
FROM {{ ref('orders') }}

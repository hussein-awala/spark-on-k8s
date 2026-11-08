{{ config(
    materialized='incremental',
    file_format='parquet',
    incremental_strategy='append'
) }}
SELECT
    cast(seed_orders.id AS bigint) AS id,
    seed_orders.customer,
    cast(seed_orders.amount AS bigint) AS amount,
    reflect('java.lang.System', 'getenv', 'HOSTNAME') AS executor_host
FROM {{ ref('orders') }} AS seed_orders
{% if is_incremental() %}
WHERE
    seed_orders.id > (
        SELECT coalesce(max(prior_orders.id), 0)
        FROM {{ this }} AS prior_orders
    )
{% endif %}

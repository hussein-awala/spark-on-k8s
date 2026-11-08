SELECT
    count(*) AS order_count,
    sum(amount) AS total_amount
FROM {{ ref('orders_table') }}

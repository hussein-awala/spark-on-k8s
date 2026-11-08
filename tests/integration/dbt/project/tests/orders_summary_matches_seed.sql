SELECT
    summary.order_count,
    summary.total_amount
FROM {{ ref('orders_summary') }} AS summary
WHERE summary.total_amount != (
    SELECT sum(seed.amount) FROM {{ ref('orders') }} AS seed
)

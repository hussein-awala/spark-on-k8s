{{ config(
    enabled=var('exercise_cancel', false),
    materialized='table',
    file_format='parquet'
) }}
SELECT count(*) AS total FROM range(0, 1000000000000, 1, 4)
WHERE id % 7 = 0

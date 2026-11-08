{{ config(enabled=var('exercise_failure', false)) }}
SELECT definitely_missing_column FROM range(1)

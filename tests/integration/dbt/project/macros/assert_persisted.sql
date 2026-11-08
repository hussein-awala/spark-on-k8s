{% macro assert_persisted(
    expected_count=3, expected_sum=60, expected_hosts=[]
) %}
{% for table in ['orders_table', 'orders_incremental'] %}
{% set query = 'SELECT COUNT(*), SUM(amount) FROM '
    ~ target.schema ~ '.' ~ table %}
{% set rows = run_query(query) %}
{% if execute and (
    rows.rows[0][0] | int != expected_count | int
    or rows.rows[0][1] | int != expected_sum | int
) %}
{{ exceptions.raise_compiler_error('Incorrect persisted count/sum: ' ~ table) }}
{% endif %}
{% endfor %}
{% if expected_hosts | length %}
{% set query = 'SELECT DISTINCT executor_host FROM '
    ~ target.schema ~ '.orders_incremental' %}
{% set hosts = run_query(query) %}
{% if execute %}
{% for host in hosts.rows %}
{% if host[0] not in expected_hosts %}
{{ exceptions.raise_compiler_error('SQL was not run on an owned executor') }}
{% endif %}
{% endfor %}
{% endif %}
{% endif %}
{% set query = 'DESCRIBE EXTENDED ' ~ target.schema ~ '.orders_incremental' %}
{% set locations = run_query(query) %}
{% if execute %}
{% set found = namespace(shared=false) %}
{% for row in locations.rows %}
{% if row[0] | trim == 'Location' and row[1].startswith('hdfs://') %}
{% set found.shared = true %}
{% endif %}
{% endfor %}
{% if not found.shared %}
{{ exceptions.raise_compiler_error('Expected a shared HDFS warehouse') }}
{% endif %}
{{ log('PERSISTED_TABLE_VERIFIED count=' ~ expected_count
    ~ ' sum=' ~ expected_sum, info=true) }}
{% endif %}
{% endmacro %}

{# Validate language before the Spark materialization can run hooks or drop a target. #}
{% materialization table, adapter='spark_on_k8s', supported_languages=['sql'] %}
  {{ return(context['materialization_table_spark']()) }}
{% endmaterialization %}

{% materialization incremental, adapter='spark_on_k8s', supported_languages=['sql'] %}
  {{ return(context['materialization_incremental_spark']()) }}
{% endmaterialization %}

{% materialization clone, adapter='spark_on_k8s', supported_languages=['sql'] %}
  {{ exceptions.raise_compiler_error('The experimental spark_on_k8s adapter does not support dbt clone') }}
{% endmaterialization %}

{% macro get_materialization(prod_mat='incremental', dev_mat='table') %}
  {{ return(prod_mat if target.name == 'prod' else dev_mat) }}
{% endmacro %}
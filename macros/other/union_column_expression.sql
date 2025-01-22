{% macro union_column_expression(col_type, col_name, relation_columns, relation) %}
    {%- if col_name is not none and col_name in relation_columns[relation] -%}
        {{ adapter.quote(col_name) }}
    {%- elif 'array' in col_type | lower -%}
        array()  {# Возвращаем пустой массив для типа данных array #}
    {%- elif 'String' in col_type -%}
        ''  {# Возвращаем пустую строку для типа String #}
    {%- else -%}
        0  {# Возвращаем 0 для других типов #}
    {%- endif -%}
{% endmacro %}
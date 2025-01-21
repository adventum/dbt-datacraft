{%- macro link(
  params = none,
  disable_incremental=none,
  override_target_model_name=none,
  date_from = none,
  date_to = none,
  datasources=none,
  limit0=none
  ) 
-%}


{#- задаём части имени - чтобы выделить имя нужной hash-таблицы -#}
{%- set model_name_parts = (override_target_model_name or this.name).split('_') -%}
{%- set hash_raw_name = model_name_parts[1:] -%}
{%- set hash_name = '_'.join(hash_raw_name) -%}
{% set source_model_name = 'hash_' ~ hash_name %}

{#- задаём пустой список: сюда будем добавлять колонки, по которым будем делать GROUP BY -#}
{% set group_by_fields = [] %}

{#- задаём по возможности инкрементальность -#}

{%- set columns_names_with_date_type = [] -%}
{%- set source_columns = adapter.get_columns_in_relation(load_relation(ref(source_model_name))) -%}
{%- for c in source_columns -%}
{%- if 'Date' in c.data_type or 'DateTime' in c.data_type -%}
{%- do columns_names_with_date_type.append(c.name)  -%}
{%- endif -%}
{%- endfor -%} 
{%- if '__date' in columns_names_with_date_type -%}

{{ config(
    materialized='incremental',
    order_by=('__date', '__table_name'),
    incremental_strategy='delete+insert',
    unique_key=['__date', '__table_name'],
    on_schema_change='fail'
) }}

{#- если не установлено - будем делать table -#}
{%- else -%}

{{ config(
    materialized='table'
) }}

{%- endif -%}

{#- задаём наименования числовых типов данных -#}
{%- set numeric_types = ['UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt256', 
                        'Int8', 'Int16', 'Int32', 'Int64', 'Int128', 'Int256',
                        'Float8', 'Float16','Float32', 'Float64','Float128', 'Float256','Num'] -%} 

{#- меняем немного логику, так как при таком подходе, если все поля не числовые или почти все, то группировка идёт по всем полям и дубли не убираются из даннных -#}
{#- для каждой колонки таблицы, на которую будем ссылаться -#}
{# SELECT {% for c in source_columns -%} #}
{#- если тип данных колонки не числовой - добавляем её в список для будущей группировки -#}
{# {% if c.data_type not in numeric_types %} {{ c.name }} #}
{# {%- do group_by_fields.append(c.name)  -%} #}
{#- а если тип данных колонки числовой - суммируем данные по ней -#}
{# {%- elif c.data_type in numeric_types %} SUM({{ c.name }}) AS {{ c.name }} #}
{#- а для остальных (т.е. колонок не числового типа данных) - делаем MAX - чтобы избежать дублей -#}
{# {%- else %} MAX({{ c.name }}) AS {{ c.name }} #}
{#- после каждой строки кроме последней расставляем запятые, чтобы сгенерировался читаемый запрос -#}
{# {%- endif %}{% if not loop.last %},{% endif %}{% endfor %} #}
{# FROM {{ ref(source_model_name) }} #}
{# GROUP BY {{ group_by_fields | join(', ') }} #}


SELECT {% for c in source_columns -%}
{# Столбец, по которому группируем, просто выводим #}
{% if c.name == '__id' %}
      {{ c.name }} AS {{ c.name }}
{#- для колонок не числового типа данных - делаем any() - чтобы избежать дублей -#}
{#- выбрали функцию any(), так как её использование позволяет улучшить производительность в отличии от ф-ии max() -#}
{% elif c.data_type not in numeric_types %} 
      any({{ c.name }}) AS {{ c.name }}
{#- а если тип данных колонки числовой - суммируем данные по ней, но не для всех источников такая схема -#}
{%- elif c.data_type in numeric_types -%}
    {# Определяем список источников, для которых будем использовать any() вместо SUM() #}
    {%- set excluded_sources = ['amocrm', 'profitbase'] -%}

    {# Проверяем, был ли передан аргумент datasources #}
    {%- if datasources is not none -%}
        {# Приводим элементы datasources к нижнему регистру и оставляем только те, которые есть в excluded_sources #}
        {%- set matching_sources = datasources | map('lower') | select("in", excluded_sources) | list -%}
        {# {{ log("matching_sources: " ~ matching_sources, info=True) }} #}
    {%- else -%}
        {# Если datasources не задан, считаем, что пересечений нет (matching_sources — пустой список) #}
        {%- set matching_sources = [] -%}
    {%- endif -%}

    {# Если пересечение (matching_sources) не пустое, применяем any(), иначе SUM() #}
    {%- if matching_sources | length > 0 -%}
        {# Для исключённых источников используем any(), чтобы избежать дублирования значений #}
        any({{ c.name }}) AS {{ c.name }}
    {%- else -%}
        {# Для остальных источников считаем сумму значений в столбце #}
        SUM({{ c.name }}) AS {{ c.name }}
    {%- endif -%}
{%- else %} any({{ c.name }}) AS {{ c.name }}
{#- после каждой строки кроме последней расставляем запятые, чтобы сгенерировался читаемый запрос -#}
{%- endif %} {% if not loop.last %},{% endif %}{% endfor %} 
FROM {{ ref(source_model_name) }}
{#- группируем данные по главному хэшу, так как если главный хэш встречается несколько раз в данных - то это дубли -#}
GROUP BY __id

{% if limit0 %}
LIMIT 0
{%- endif -%}

{% endmacro %}
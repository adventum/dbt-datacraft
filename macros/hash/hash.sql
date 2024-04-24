{%- macro hash(
  params = none,
  disable_incremental=none,
  override_target_model_name=none,
  date_from = none,
  date_to = none
  ) -%}

{#- задаём части имени - pipeline это например datestat -#}
{%- set model_name_parts = (override_target_model_name or this.name).split('_') -%}
{%- set pipeline_name = model_name_parts[-1] -%}

{#- задаём по возможности инкрементальность -#}
{%- if pipeline_name in ('datestat', 'events', 'periodstat') -%}

{{ config(
    materialized='incremental',
    order_by=('__date', '__table_name'),
    incremental_strategy='delete+insert',
    unique_key=['__date', '__table_name'],
    on_schema_change='fail'
) }}

{%- else -%}

{{ config(
    materialized='table',
    order_by=('__table_name'),
    on_schema_change='fail'
) }}

{%-endif -%}

{%- set metadata = fromyaml(etlcraft.metadata()) -%}

{#- если имя модели не соответствует шаблону - выдаём ошибку -#}
{%- if model_name_parts|length < 2 or model_name_parts[0] != 'hash' -%}
{{ exceptions.raise_compiler_error('Model name "' ~ this.name ~ '" does not follow the expected pattern: "hash_{pipeline_name}"') }}
{%- endif -%}

{#- задаём паттерн, чтобы найти combine-таблицу нужного пайплайна -#}

{%- if pipeline_name == 'registry' -%}
    {%- set table_pattern = 'combine_' ~ model_name_parts[1] ~'_'~ pipeline_name -%}  
{%- else -%}
    {%- set table_pattern = 'combine_' ~ pipeline_name -%}
{%- endif -%}

{#- находим все таблицы, которые соответствут паттерну -#}
{%- set relations = etlcraft.get_relations_by_re(schema_pattern=target.schema, 
                                                              table_pattern=table_pattern) -%} 

{#- собираем одинаковые таблицы, которые будут проходить по этому макросу  - здесь union all найденных таблиц -#}
{%- set source_table = '(' ~ etlcraft.custom_union_relations(relations) ~ ')' -%}

{#- задаём список всех линков -#}
{%- set links = metadata['links'] -%}
{#- задаём списки, куда будем отбирать линки и сущности -#}
{%- set links_list = [] -%}
{%- set entities_list = [] -%}
{%- set registry_main_entities_list = [] -%}
{#- отбираем нужные линки и их сущности -#}
{%- if pipeline_name !='registry' -%}
{%- for link_name in links  -%}
    {%- set link_pipeline = links[link_name].get('pipeline') -%}
    {%- set datetime_field = links[link_name].get('datetime_field') -%}
    {%- set main_entities = links[link_name].get('main_entities') or [] -%}
    {%- set other_entities = links[link_name].get('other_entities') or [] -%}
    {%- set entities = main_entities + other_entities -%}
    {%- if link_pipeline == pipeline_name -%}
        {%- do links_list.append(link_name) -%}
        {%- for entity in entities -%}
            {%- do entities_list.append(entity) -%}
        {%- endfor -%}
    {%- endif -%} 
    {%- for  main_entity in main_entities -%}
        {%- if link_pipeline == 'registry'  -%}
            {%- do registry_main_entities_list.append(main_entity) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}
{%- endif -%}

{#- условие либо glue=yes, либо сущность из main_entities -#}

{#- находим сущности с glue=yes -#}
{#- задаём список всех сущностей метадаты -#}
{%- set metadata_entities = metadata['entities'] -%}
{#- задаём список для сущностей, у которых найдём glue='yes' -#}
{%- set metadata_entities_list = [] -%}
{#- отбираем нужные сущности -#}
{%- for entity_name in metadata_entities  -%}
    {%- set entity_glue = metadata_entities[entity_name].get('glue') -%}
    {%- if entity_glue -%}  {# по факту читается как True, поэтому пишем просто if #}
        {%- do metadata_entities_list.append(entity_name) -%}
    {%- endif -%} 
{%- endfor -%}

{#- делаем полученный список сущностей уникальным -#}
{%- set unique_entities_list = entities_list|unique|list -%}


{#- из уникального списка сущностей отбираем те, которые 
    либо есть в списке сущностей glue='yes', либо есть в разделе registries -#}
{%- set final_entities_list = [] -%}
{%- for unique_entity in unique_entities_list  -%}
    {%- if unique_entity in registry_main_entities_list or unique_entity in metadata_entities_list -%}
        {%- do final_entities_list.append(unique_entity) -%}
    {%- endif -%}
{%- endfor -%}


{#- для моделей пайплайна registry отбираем линки и сущности отдельно, 
чтобы выводить модели по-отдельности для каждого источника данных -#}
{%- if pipeline_name == 'registry' -%}
{#- для названия линка берём то значение, что указано начиная с шага join в поле __link -#}
{%- set links_list = dbt_utils.get_column_values(table=ref(table_pattern), column='__link',max_records=1) -%}
{#- для этого линка отбираем связанные с ним сущности -#}
{%- set final_entities_list = [] -%}
{%- for link_name in links_list  -%}
    {%- set main_entities = links[link_name].get('main_entities') or [] -%}
    {%- set other_entities = links[link_name].get('other_entities') or [] -%}
    {%- set entities = main_entities + other_entities -%}
    {%- for entity in entities -%}
        {%- do final_entities_list.append(entity) -%}
    {%- endfor -%}
{%- endfor -%} 
{#- делаем полученные списки уникальными -#}
{%- set links_list = links_list|unique|list -%}
{%- set final_entities_list = final_entities_list|unique|list -%}
{%- endif -%}

{#- основной запрос -#} 

SELECT *,
  assumeNotNull(CASE 
{% for link in links_list %}
    {%- set link_hash = link ~ 'Hash' -%}  
    WHEN __link = '{{link}}' 
    THEN {{link_hash}} 
{% endfor %}
    END) as __id
  , assumeNotNull(CASE 
{% for link_name in links  %}
    {%- set datetime_field = links[link_name].get('datetime_field') -%}
    {%- set link_pipeline = links[link_name].get('pipeline') -%}
    {%- if link_pipeline == pipeline_name -%}
        WHEN __link = '{{link_name}}' 
        THEN toDateTime({{datetime_field}})
    {% endif %}
{% endfor %}
    END) as __datetime

FROM (

SELECT 
    *, 
    {% for link in links_list %}
        {# добавляем хэши для отобранных линков #}
        {{ etlcraft.link_hash(link, metadata) }}{% if not loop.last %},{% endif -%}  {# ставим запятые везде, кроме последнего элемента цикла #}
    {% endfor %}
    {%- if final_entities_list and links_list -%},{%- endif -%} {# если есть сущности, ставим перед их началом запятую #}
    {% for entity in final_entities_list %}
        {# добавляем хэши для отобранных сущностей #}
        {{ etlcraft.entity_hash(entity, metadata) }}{% if not loop.last %},{% endif -%} {# ставим запятые везде, кроме последнего элемента цикла #}
    {% endfor %}
FROM {{ source_table }} 
    WHERE 
    {% for link in links_list %}
        {{ link ~ 'Hash' != ''}}{% if not loop.last %} AND {% endif -%}
    {% endfor %}
    )

-- SETTINGS short_circuit_function_evaluation=force_enable

{% endmacro %}
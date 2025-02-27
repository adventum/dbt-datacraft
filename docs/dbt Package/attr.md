---
category: main
step: 6_attribution
sub_step: 
doc_status: ready
language: rus
main_number: "09"
---
# macro `attr`

## Список используемых вспомогательных макросов
| Name                                   | Category | In Main Macro | Doc Status |
| -------------------------------------- | -------- | ------------- | ---------- |
| [[attr_prepare_with_qid]]              | sub_main | attr          | ready      |
| [[attr_create_events]]                 | sub_main | attr          | ready      |
| [[attr_add_row_number]]                | sub_main | attr          | ready      |
| [[attr_find_new_period]]               | sub_main | attr          | ready      |
| [[attr_calculate_period_number]]       | sub_main | attr          | ready      |
| [[attr_create_missed_steps]]           | sub_main | attr          | ready      |
| [[attr_join_to_attr_prepare_with_qid]] | sub_main | attr          | ready      |
| [[attr_model]]                         | sub_main | attr          | ready      |
| [[attr_final_table]]                   | sub_main | attr          | ready      |

## Описание

Макрос `attr` предназначен для подготовки и анализа данных для атрибуции. Он реализуется в несколько шагов:
1. [[attr_prepare_with_qid]]
2. [[attr_create_events]]
3. [[attr_add_row_number]]
4. [[attr_find_new_period]]
5. [[attr_calculate_period_number]]
6. [[attr_create_missed_steps]]
7. [[attr_join_to_attr_prepare_with_qid]]
8. [[attr_model]]
9. [[attr_final_table]]
## Применение

Имя dbt-модели (=имя файла в формате sql в папке models) должно соответствовать шаблону:
`attr_{название_модели атрибуции}_{название_шага}`.

Например, `attr_myfirstfunnel_prepare_with_qid`.

Внутри этого файла вызывается макрос:

```sql
{{ datacraft.attr() }}
```
Над вызовом макроса в файле будет указана зависимость данных через `—depends_on`. То есть целиком содержимое файла выглядит, например, вот так:
```sql
-- depends_on: {{ ref('full_events') }}

-- depends_on: {{ ref('graph_qid') }}

{{ datacraft.attr() }}
```
## Аргументы

Этот макрос принимает следующие аргументы:

1. `params` (по умолчанию: none)
2. `override_target_model_name` (по умолчанию: none)
3. `limit0` (по умолчанию: none)

## Функциональность

Технически сам макрос `attr` - регулировщик. Он направляет работу под-макросов типа `attr_` по шагам. 

Под-макросами являются:
1. [[attr_prepare_with_qid]]
2. [[attr_create_events]]
3. [[attr_add_row_number]]
4. [[attr_find_new_period]]
5. [[attr_calculate_period_number]]
6. [[attr_create_missed_steps]]
7. [[attr_join_to_attr_prepare_with_qid]]
8. [[attr_model]]
9. [[attr_final_table]]

Технически действие самого макроса `attr`(регулировщика) реализуется так: 

сначала макрос считает имя модели - либо из передаваемого аргумента (  
`override_target_model_name`), либо из имени файла (`this.name`). При использовании аргумента `override_target_model_name` макрос работает так, как если бы находился в модели с именем, равным значению `override_target_model_name`.

Из имени модели макрос берёт название воронки - `funnel_name`.

Затем макрос вызывает нужный шаг макроса `attr` в зависимости от считанного имени модели. Название воронки макрос передаёт дальше - в свой под-макрос - как аргумент.
## Пример

Файл в формате sql в папке models. Название файла `attr_myfirstfunnel_prepare_with_qid`

Содержимое файла:
```sql
-- depends_on: {{ ref('full_events') }}

-- depends_on: {{ ref('graph_qid') }}

{{ datacraft.attr() }}
```

## Примечания

Это девятый из основных макросов.
---
category: main
step: 1_silos
sub_step: 1_normalize
doc_status: ready
language: rus
main_number: "01"
---
# macro `normalize`
## Список используемых вспомогательных макросов

| Name                                | Category  | In Main Macro          | Doc Status |
| ----------------------------------- | --------- | ---------------------- | ---------- |
| [[get_from_default_dict]]           | auxiliary | normalize              | ready      |
| [[normalize_name]]                  | auxiliary | normalize              | ready      |
| [[json_extract_string]]             | auxiliary | normalize              | ready      |
| [[custom_union_relations_source]]   | auxiliary | normalize              | ready      |
| [[find_incremental_datetime_field]] | auxiliary | normalize, incremental | ready      |
| [[get_from_default_dict]]           | auxiliary | normalize              | ready      |
| [[get_relations_by_re]]             | auxiliary | normalize, combine     | ready      |
| [[json_extract_string]]             | auxiliary | normalize              | ready      |
| [[normalize_name]]                  | auxiliary | normalize              | ready      |


## Описание

Макрос `normalize` предназначен для нормализации таблиц, загруженных Airbyte.

## Применение

Имя dbt-модели (=имя файла в формате sql в папке models) должно соответствовать шаблону:
`normalize_{название_источника}_{название_пайплайна}_{название_шаблона}_{название_потока}`.

Например, `normalize_appmetrica_events_default_deeplinks`.

Внутри этого файла вызывается макрос:

```sql
{{ datacraft.normalize() }}
```
## Аргументы

Этот макрос принимает следующие аргументы:

01. `fields` (обязательный аргумент - список полей)
02. `incremental_datetime_field` (по умолчанию: none)
03. `incremental_datetime_formula` (по умолчанию: none)
04. `disable_incremental_datetime_field` (по умолчанию: none)
05. `defaults_dict` (по умолчанию: результат макроса fieldconfig())
06. `schema_pattern` (по умолчанию 'airbyte_internal')
07. `source_table` (по умолчанию: none)
08. `override_target_model_name` (по умолчанию: none)
09. `debug_column_names` (по умолчанию False)
10. `old_airbyte` (по умолчанию True)
11. `limit0` (по умолчанию: none)

## Функциональность

Этот макрос должен начинать свою работу после выведения зависимостей. То есть после того, как прошёл первый этап формирования проекта - `dbt parse`, на котором создаётся `manifest`. После того, как `manifest` создан, в проекте уже смогут быть использованы внутренние ссылки - `ref`, которые необходимы для работы dbt-моделей.
 
Поэтому макрос `normalize` обёрнут в условие `if execute` - иначе его работа может пройти впустую.

Первым делом в макросе задаются части имени - либо из передаваемого аргумента (`override_target_model_name`), либо из имени файла (`this.name`). При использовании аргумента `override_target_model_name` макрос работает так, как если бы находился в модели с именем, равным значению `override_target_model_name`.

Длинное название, полученное тем или иным способом, разбивается на части по знаку нижнего подчёркивания. Например, название `normalize_appmetrica_events_default_deeplinks` разобьётся на 5 частей.

Если имя модели не соответствует шаблону (не начинается с `normalize_`, или в нём не хватает частей) - макрос не идёт дальше и на этом шаге уже выводит для пользователя ошибку с кратким описанием проблемы.

Далее макрос задаёт переменные источника, пайплайна, шаблона и потока. Для примера `normalize_appmetrica_events_default_deeplinks` это будет:
- источник - `sourcetype_name` → appmetrica
- пайплайн - `pipeline_name` → events
- шаблон - `template_name` → default
- поток - `stream_name` → deeplinks
  
После этого макрос собирает из этих частей паттерн для поиска соответствующих модели таблиц с “сырыми” данными.  Вот сам паттерн:

 `'[^_]+' ~ '_[^_]+_' ~ 'raw__stream_' ~ sourcetype_name ~ '_' ~ template_name ~ '_[^_]+_' ~ stream_name ~ '$'`
 
В него подставляются значения из переменных, и в итоге макрос сможет найти для примера с `normalize_appmetrica_events_default_deeplinks` данные из `datacraft_clientname_raw__stream_appmetrica_default_accountid_deeplinks` (потому что название “сырых” данных соответствует шаблону).

Если пайплайн соответствует направлениям `registry` или `periodstat`, то автоматически задаётся аргумент `disable_incremental_datetime_field`, равный `True`. Это значит, что в таких данных нет инкрементального поля с датой, и для этих данных макрос не будет пытаться искать такое поле.

Далее макрос будет искать “сырые” данные для модели, в которой он вызывается. Название таблицы-источника с нужными “сырыми” данными можно задать напрямую при вызове макроса - за это отвечает аргумент `source_table`. 

Если параметр `source_table` при вызове макроса не задан, то мы ищем `relations` - то есть связи с необходимыми таблицами - при помощи собственного макроса [[get_relations_by_re]]. Он находится в файле `clickhouse-adapters`. Этот макрос помогает найти все таблицы, которые подходят под единый шаблон (например, все данные из `appmetrica` для какого-либо проекта).

Внутри этого макроса есть аргумент `schema_pattern`, который можно задавать при вызове макроса `normalize`. Если сырые данные лежат в той же схеме, что и модель, то `schema_pattern=target.schema`.Если сырые данные идут из Airbyte новой версии, то они пишутся в отдельную схему `airbyte_internal`.Поэтому по умолчанию у нас задан `schema_pattern='airbyte_internal'`.

Если что-то не так с поиском `relations` - макрос не пойдёт дальше и выдаст пользователю ошибку с описанием проблемы.
                                                                 
После того, как нужные “сырые” данные найдены, макрос собирает воедино все найденные таблицы через `UNION ALL`. Для этого используется макрос [[custom_union_relations_source]], внутрь которого передаются ранее найденные `relations`.

Далее для тех данных, у которых не указано отсутствие инкрементального поля с датой (то есть аргументы `incremental_datetime_formula` и `disable_incremental_datetime_field` оставлены как по умолчанию - `none`), происходит формулы для поля с датой - `incremental_datetime_formula`. Для поиска используется макрос [[dbt Package/get_from_default_dict]], внутрь которого передаётся аргумент `defaults_dict`. Этот аргумент задан по умолчанию как результат вызова ещё одного макроса - `fieldconfig()`. Таким образом, по умолчанию весь процесс происходит автоматически, пользователю ничего не нужно делать. Но при этом у пользователя есть возможность при необходимости воздействовать на поведение макроса.
  
Также макросом устанавливается `incremental_datetime_field` при помощи макроса [[find_incremental_datetime_field]].

Далее происходит обработка полей. На вход макросу `normalize` передаётся список полей - `fields`. Для каждого элемента этого списка (кроме инкрементального поля с датой) происходит обработка - макрос создаём псевдоним, делая транслитерацию на английский при помощи макроса  [[dbt Package/normalize_name]]. 

При помощи макроса [[dbt Package/json_extract_string]] устанавливаются значения названий полей из технического поля Airbyte `'_airbyte_data'`, если аргумент `debug_column_names` оставлен по умолчанию как `False`. Если аргумент `True`, то будет браться ранее созданный псевдоним. Полученный список полей сортируется по алфавиту.

Инкрементальное поле с датой обрабатывается отдельно - и для всех случаев его название становится универсальным: `__date`. 

Если в результате всех преобразований список полей получился пустым, макрос прервёт свою работу и выдаст пользователю краткое описание ошибки.

Итоговый список полей далее передаётся в автоматически генерируемыймый SQL-запрос. Все поля перечисляются в блоке SELECT. Кроме колонок с данными в этот запрос ещё входят:

- поле `__table_name` (обёрнутое в toLowCardinality()) - здесь указывается название таблицы, откуда были взяты данные
  
- поле `__emitted_at` (в формате DateTime). В зависимости от значения аргумента `old_airbyte` макрос возьмёт либо поле `_airbyte_extracted_at` (если значение аргумента `True`, как задано по умолчанию), либо `_airbyte_emitted_at` (если значение `False`). Это поле содержит информацию о времени извлечения сырых данных.
  
- поле `__normalized_at` (формируется через NOW()). Это поле содержит информацию о времени нормализации данных.
  
Все данные берутся из ранее созданной `source_table`.
  
Если активирован аргумент `limit0` (который по умолчанию установлен как `none`), то в конце запроса будет прописан LIMIT 0.
  
## Пример

Файл в формате sql в папке models. Название файла `normalize_appmetrica_events_default_deeplinks`

Содержимое файла:
```sql
{{ datacraft.normalize(
fields=['__clientName','__productName','appmetrica_device_id','city',
'deeplink_url_parameters','event_receive_datetime','google_aid',
'ios_ifa','os_name','profile_id','publisher_name']
) }}
```
## Примечания

Это первый из основных макросов.

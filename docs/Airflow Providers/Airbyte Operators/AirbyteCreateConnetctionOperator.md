---
api_version: Официальное API
description: 
type: operator
doc_status: ready
status: ready to test
---
## Описание
Создает новый [[Airbyte Connection]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `name` — название Airbyte Connection, которое нужно создать
- `source_id` — ID [[Airbyte Source]], который мы хотим связать с destination(БД). 
- `destination_id` — ID  [[Airbyte Destination]], который мы хотим связать с source выше. 
- словарь `params`, который содержит следующие параметры:
    - `syncCatalog` — параметры стримов
    - `schedule` — параметры расписания запуска
    - `namespaceDefinition` — правила нейминга таблиц с данными: `destination` (по умолчанию), `source` или `custom_format`. Если задан `custom_format`, то можно также задать `namespaceFormat`
    - `prefix` — префикс, который будет прибавляться к названиям таблиц
    - `nonBreakingSchemaUpdatesBehavior` (по умолчанию `ignore`)
    - `status` — `active`, `inactive` или `deprecated`
## Возвращаемое значение
Словарь с параметрами созданного Airbyte Connection, в т. ч. `connectionId`.
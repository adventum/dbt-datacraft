---
api_version: Официальное API
description: 
type: operator
doc_status: ready
status: не готово
---
## Описание
Создает новый [[Airbyte Source]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `workspace_id` или `workspace_name` + `workspaces_list`(см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `source_definition_id` — ID source_definition , который нужно использовать. Можно получить после использования оператора AirbyteCreateSourceDefinitionsOperator 
- `connection_configuration` — параметры (свои для каждого коннектора).
- `name` - имя коннектора (source)
## Возвращаемое значение
Словарь с параметрами созданного коннектора, в т. ч. `sourceId`.
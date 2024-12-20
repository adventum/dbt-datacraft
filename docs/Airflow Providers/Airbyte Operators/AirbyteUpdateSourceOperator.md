---
api_version: Официальное API
description: 
type: operator
doc_status: ready
status: не готово
---
## Описание
Обновляет имеющийся [[Airbyte Source]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `workspace_id` или `workspace_name` + `workspaces_list`(см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `source_id` — ID  Airbyte Source, который нужно обновить
- `connection_configuration` — параметры (свои для каждого коннектора).
- `name` - имя коннектора
## Возвращаемое значение
Словарь с параметрами обновленного коннектора, в т. ч. `sourceId`.
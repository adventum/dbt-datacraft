---
api_version: Официальное API
description: 
type: operator
doc_status: ready
status: не готово
---
## Описание
Обновляет имеющийся [[Airbyte Destination]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `workspace_id` или `workspace_name` + `workspaces_list`(см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `destination_id` — ID  Airbyte Destination, который нужно обновить.
- `connection_configuration` - словарь с параметрами самого destination(например `host`, `port`, `user`, `password` ...) - для каждого destination индивидуальные параметры
- `name` - имя destination
## Возвращаемое значение
Словарь с параметрами обновленного коннектора, в т. ч. `destinationId`.
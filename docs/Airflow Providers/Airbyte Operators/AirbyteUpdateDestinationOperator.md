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
- `id` или `name`— ID или название Airbyte Destination, который нужно обновить. Если передан аргумент `name`, то  становится обязательным аргумент `destinations_list`. В этот аргумент нужно передать результат вызова оператора [[AirbyteListDestinationsOperator]].
- `configuration` — параметры (свои для каждого коннектора).
## Возвращаемое значение
Словарь с параметрами обновленного коннектора, в т. ч. `destinationId`.
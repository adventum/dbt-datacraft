---
api_version: Официальное API
description: 
type: operator
doc_status: ready
status: не готово
---
## Описание
Создает новый [[Airbyte Destination]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `workspace_id` или `workspace_name` + `workspaces_list`(см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `destination_definition_id` — ID созданного destination_definition. Его можно получить после использования AirbyteCreateDestinationDefinitionsOperator
- `connection_configuration` — (словарь) параметры (свои для каждого коннектора).
- `name` - имя для destination (БД)
## Возвращаемое значение
Словарь с параметрами созданного коннектора, в т. ч. `destinationId`.
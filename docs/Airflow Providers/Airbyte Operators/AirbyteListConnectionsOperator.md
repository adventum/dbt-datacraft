---
api_version: Официальное API
description: 
type: operator
doc_status: ready
status: не готово
---
## Описание
Возвращает информацию по имеющимся [[Airbyte Source]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `workspace_id` или `workspace_name` + `workspaces_list` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
## Возвращаемое значение
Список словарей с полями:
- `connectionsId`,
- `name`
- `sourceId`
- `destinationId`
- `status`
- `schedule`
- `nonBreakingChangesPreference`
- `namespaceDefinition`
- `namespaceFormat`
- `prefix`
- `configurations`
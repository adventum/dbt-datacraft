---
api_version: Устаревшее API
description: 
type: operator
doc_status: ready
status: ready (нужен тест?)
---
## Описание
Возвращает информацию по имеющимся [[Terms/DAG|коннекторам]] для [[Airbyte Source]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `workspace_id` или `workspace_name` + `workspaces_list` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
## Возвращаемое значение
Список словарей с полями:
- `sourceDefinitionId`
- `name` 
- `dockerRepository`
- `dockerImageTag`
- `documentationUrl`
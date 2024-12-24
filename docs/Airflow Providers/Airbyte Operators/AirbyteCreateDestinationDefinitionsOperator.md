---
api_version: Устаревшее API
description: 
type: operator
doc_status: ready
status: не готово
---
## Описание
Создает новый [[Terms/DAG|коннектор]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `workspace_id` или `workspace_name` + `workspaces_list` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- словарь `destination_definition_configuration` , он должен содержать ключи:
	- `name`— название коннектора, который нужно создать.
	-  `dockerRepository` - ссылка на репозиторий (например: airbyte/destination-clickhouse)
	- `dockerImageTag` — версия используемого коннектора (например: 1.0.0)
	- `documentationUrl` — ссылка на документацию
## Возвращаемое значение
Словарь с параметрами созданного коннектора, в т. ч. `destinationDefinitionId`.
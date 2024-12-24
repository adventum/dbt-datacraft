---
api_version: Устаревшее API
description: 
type: operator
doc_status: ready
status: не готово
---
## Описание
Обновляет имеющийся [[Terms/DAG|коннектор]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `id` — ID коннектора, который нужно обновить. 
- source_definition_configuration - словарь с параметрами:
	 `name` - имя source_definition
	- `dockerRepository` - ссылка на используемый ресурс коннектора БД (например: adventum/source-google-sheets)
	- `dockerImageTag` - тег коннектора (например 1.0.0)
	- `documentationUrl` - ссылка на документацию коннектора БД

## Возвращаемое значение
Словарь с параметрами обновленного Airbyte Definition, в т. ч. `sourceDefinitionId`.
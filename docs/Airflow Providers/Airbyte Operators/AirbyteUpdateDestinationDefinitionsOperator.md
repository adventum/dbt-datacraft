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
- `destination_definition_id` — ID коннектора, который нужно обновить. 
- `destination_definition_configuration` - словарь с параметрами для обновления(Как я понял, обновить можно только dockerImageTag):
	- `name` - имя destination_definition
	- `dockerRepository` - ссылка на используемый ресурс коннектора БД (например: airbyte/destination-clickhouse)
	- `dockerImageTag` - тег коннектора БД (например 1.0.0)
	- `documentationUrl` - ссылка на документацию коннектора БД
## Возвращаемое значение
Словарь с параметрами обновленного Airbyte Definition, в т. ч. `destinationDefinitionId`.
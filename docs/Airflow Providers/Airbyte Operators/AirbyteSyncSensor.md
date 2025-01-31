---
api_version: Официальное API
description: 
type: operator
doc_status: ready
status: не готово
---
## Описание
Ждет окончания синхронизации для [[Airbyte Connection]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `job_id` — ID задачи на синхронизацию, можно получить из оператора AirbyteTriggerSyncOperator
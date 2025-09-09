---
api_version: Официальное API
description: 
type: operator
doc_status: ready
status: не готово
---
## Описание
Возвращает информацию по имеющимся задачам на синхронизацию.
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `connection_id` или `connections_name`— ID или название Airbyte Connection, который нужно обновить.
- statuses — список статусов задач, которые нужно оставить. Допустимые значения: `pending`, `running`, `incomplete`, `failed`, `succeeded`, `cancelled`. Значение по умолчанию: `[pending, running]`.
- `config_types` - список типов Job's. Допустимые значения:  `check_connection_source`, `check_connection_destination`, `discover_schema`, `get_spec`, `sync`, `reset_connection`, `refresh`, `clear`
## Возвращаемое значение
Список словарей с полями:
- `jobId`,
- `status`
- `jobType`
- `connectionId`
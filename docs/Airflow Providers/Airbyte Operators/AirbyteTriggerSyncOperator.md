---
api_version: Официальное API
description: 
type: operator
doc_status: ready
status: не готово
---
## Описание
Активирует синхронизацию для [[Airbyte Connection]].
## Аргументы
- `airbyte_conn_id` (см. [[Airbyte Operators#Общие аргументы всех операторов|Общие аргументы]])
- `connection_id` — ID Airbyte Connection, синхронизацию которого нужно активировать.
Можно получить
## Возвращаемое значение
Словарь `job`  с параметрами задачи, `attempts` с данными о попытках
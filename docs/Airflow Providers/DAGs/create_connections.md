---
dag_name: creat_connections
description: Создаёт Airbyte Connection
status: ready to test
doc_status: ready
type: dag
---
DAG создает/обновляет вначале источники, места назначения, а потом подключения в Airbyte.

Принимает как параметр запуска поле `namespace`, которые используется для поиска метаконфигов - [[Configs#Метаконфиги]]

Правила создания:
1. Должно быть место назначения `clickhouse_defaut` со следующими параметрами на основе конфига [[base]]:
```
   "connectionConfiguration": {
        "ssl": false,
        "host": "значение переменной clickhouse_default_host",
        "port": 8123,
        "database": "значение переменной clickhouse_default_database",
        "password": "значение переменной clickhouse_default_password",
        "tcp-port": 9000,
        "username": "значение переменной clickhouse_default_user",
        "tunnel_method": {
            "tunnel_method": "NO_TUNNEL"
        }
    }
```
2. На каждый элемент из конфига [[datasources]] должен быть один источник с названием `{source_type}_{preset}_{account_id}`, в котором конфигурация берется из конфига [[presets]] (раздел `connection_presets`), с соответствующим названием. При этом нужно заменить поля `credentials_craft_host`, `credentials_craft_token` соответствующими значениями из конфига `base`, а поле `credentials_craft_token_id` — значением из конфига `datasources`, соответствующему этому источнику. Также нужно заменить поле, прописанное в конфиге [[Airflow Providers/Configs/connectors|connectors]] для данного типа источника, на значение `account_id` из `datasources`. Пример: для источника `appmetrica` прописываем значение `account_id` в поле `application_id`.
3. На каждый элемент из конфига datasources должно быть создано подключение с названием `{source_type}_{preset}_{account_id} → clickhouse_default`, которое соединяет источник из п. 2 с местом назначения из п. 1. Параметры из соединения нужно взять из раздела `connection_presets` конфига `presets`.
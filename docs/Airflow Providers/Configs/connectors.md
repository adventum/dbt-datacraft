---
description: Коннекторы (докер-образы), которые нужно подключить в Airbyte
config_default_type: "`templated_file`"
config_default_format: "`yaml`"
type: config
doc_status: ready (нужно ревью)
---
# Описание

Словарь состоит из двух частей: 
- `source_definitions`  - определение коннекторов для источников данных
- `destination_definitions` - определение коннекторов для мест назначения. 

В обоих разделах почти одинаковая структура:

- `slug` - имя создаваемого definition, который будет отображаться в UI airbyte
- `image` - (название Docker-образа на DockerHub) : (тег Docker-образа на DockerHub)
- `documentation`  — ссылка на раздел “[[Connectors]]”, где даны инструкции, как подключить коннектор в Aibyte

В `source_definitions` дополнительно входит поле `account_id_field`, которое определяет, какое поле должно изменяться при создании нового подключения по шаблону дагом [[create_connections]].

Данные из этого конфига используются для добавления в Airbyte нужных коннекторов. Это осуществляется с помощью DAG’a [[install_connectors]].
# Пример

```yaml
source_definitions:
  - slug: appmetrica
    image: adventum/source-appmetrica-logs-api:0.4.4
    documentation: example.com
    account_id_field: application_id
  - slug: appsflyer
    image: adventum/source-appsflyer:0.3.1
    documentation: example.com
    account_id_field: app_id
destination_definitions:
  - slug: clickhouse
    image: airbyte/destination-clickhouse:1.0.0
    documentation: example.com
```
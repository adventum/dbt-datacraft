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
- и `destination_definitions` - определение коннекторов для мест назначения. 

В обоих разделах одинаковая структура:

- `name` - имя создаваемого definition, который будет отображаться в UI airbyte
- `dockerRepository` - название Docker-образа на DockerHub
- `dockerImageTag` — тег Docker-образа на DockerHub
- `documentationUrl`  — ссылка на раздел “[[Connectors]]”, где даны инструкции, как подключить коннектор в Aibyte

Данные из этого конфига используются для добавления в Airbyte нужных коннекторов. Это осуществляется с помощью DAG’a [[install_connectors]].

# Пример

```yaml
source_definitions:  
  - name: google-sheets-testing  
    dockerRepository: adventum/source-google-sheets  
    dockerImageTag: 1.0.0  
    documentationUrl: example.com  
  - name: yandex-disk-testing  
    dockerRepository: adventum/source-yandex-disk  
    dockerImageTag: 0.2.0  
    documentationUrl: example.com  
destination_definitions:  
  - name: clickhouse-testing  
    dockerRepository: airbyte/destination-clickhouse  
    dockerImageTag: 1.0.0  
    documentationUrl: example.com
```
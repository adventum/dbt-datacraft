---
term_name: Airbyte Connection
description: сущности внутри Airbyte, которые можно создать вручную или по API
type: term
doc_status: ready (нужно ревью)
---
**Airbyte Connection** (соединение) —  это сущность внутри Airbyte, благодаря которой осуществляется связь между источником данных ([[Airbyte Source|Source]]) и целевым хранилищем ([[Airbyte Destination|Destination]]). 

Соединение определяет такие параметры, как частота репликации (например, ежечасно, ежедневно, вручную) и какие потоки данных реплицировать. Настройка соединения позволяет сконфигурировать следующие параметры:

- **[[Stream|Streams]] (потоки/стримы) и Fields (поля)**, которые должны быть  реплицированы из источника в место назначения. В настройках соединения можно включать и выключать стримы и поля, которые будут загружаться в место назначения:
![[select_sreams_airbyte_example 1.jpg]]

- **Sync Mode (Режим репликации)** -   как данные будут реплицироваться из источника в место назначения. Подробнее о режимах репликации в [официальной документации](https://docs.airbyte.com/using-airbyte/core-concepts/sync-modes/)
![[sync_mode_airbyte_example.jpg]]

-  **Sync Schedule (Расписание репликации)** - когда должна быть запущена репликация данных. В `Schedule type` можно выбрать один из трёх режимов: 
	- `Scheduled` - запускает репликацию через указанный интервал времени (например, каждые 24 часа, каждые 2 часа)
	- `Cron` - запускает репликацию на основе заданного пользователем выражения cron
	- `Manual` - ручной запуск репликации данных
![[sync_schedule_airbyte_example.jpg]]
	[Подробнее про Sync Schedule](https://docs.airbyte.com/using-airbyte/core-concepts/sync-schedules)

- **Destination Namespace** - место, где будут храниться реплицированные данные в месте назначения. Например, если данные из источника выгружаются в какую-то базу данных, например, Clickhouse, можно указать конкретную схему, чтобы логически сгруппировать данные. При настройке соединения в Airbyte можно выбрать выбрать три разных варианта:
![[destination_namespace_airbyte_example.jpg]]
	[Подробнее про Destination Namespace](https://docs.airbyte.com/using-airbyte/core-concepts/namespaces )

- **Stream Prefix (префикс для стримов)** - префикс, который будет добавляться к названию стрима при его записи в место назначения. Опциональная настройка, однако по методологии **dataCraft Core** префикс нужно указывать обязательно. Правило формирования префикса: 
	`{название источника}_{название шаблона}_{id аккаунта}_`. 
	Например, если стрим содержит стандартные поля для данного источника данных, то указываем в качестве префикса `{название источника}_default_{id аккаунта}_`. 
	Использование префикса позволяет упростить и сгруппировать обработку однотипных данных.
	Подробнее про правила названия источников в [[Source]].
	Про шаблон [[Template|тут]].

- **Schema Propagation** - дополнительная настройка, которая позволяет указать как Airbyte должен обрабатывать изменения в структуре данных в источнике. [Подробнее](https://docs.airbyte.com/using-airbyte/schema-change-management)


О том как создавать соединение вручную [тут](https://docs.airbyte.com/using-airbyte/getting-started/set-up-a-connection).

С помощью **dataCraft Core** Airbyte Connection можно создавать автоматически с помощью DAG’а [[create_connections]].

### Наименование соединения в Airbyte 

Формируется на основе названия [[Airbyte Source]] и [[Airbyte Destination]]. Чтобы получить корректное название соединения, необходимо правильно назвать источник, подробнее про наименование источника в [[Airbyte Source]].  

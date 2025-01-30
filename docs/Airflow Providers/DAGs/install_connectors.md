---
dag_name: install_connectors
description: устанавливает (definitions) коннекторы в Airbyte на основе данных из конфига connectors.
status: ready to test
doc_status: ready
type: dag
---

**Решаемая задача:** создаёт/обновляет Airbyte `source_definitions` и `destination_definitions` на основе данных из конфига [[Airflow Providers/Configs/connectors|connectors]].

Там где это необходимо обеспечивается [идемпотентность](https://ru.wikipedia.org/wiki/%D0%98%D0%B4%D0%B5%D0%BC%D0%BF%D0%BE%D1%82%D0%B5%D0%BD%D1%82%D0%BD%D0%BE%D1%81%D1%82%D1%8C). Это означает, что если оператор  вызвать несколько раз подряд с одними и теми же аргументами, нужное действие будет выполнено один раз. Например, если несколько раз попытаться создать Airbyte Connection с одним и тем же именем, оно будет создано только в одном экземпляре.

# Результат

Создает/Обновляет source_definitions и destination_definitions в соответствии с объектами конфига [[Airflow Providers/Configs/connectors|connectors]]
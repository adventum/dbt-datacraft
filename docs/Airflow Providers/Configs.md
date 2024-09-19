# Конфигурация

В DAG'ах **etlCraft** используется функция `get_configs(...)`, которая позволяет получать все исходные данные для запуска DAG'а. При этом она позволяет:
- иметь несколько конфигураций в одном инстансе Airflow за счет *неймспейсов*
- собирать конфиги из разных мест, за счет чего пользователь может переопределять часть переменных под свои задачи
- собирать конфиги как в формате YAML, так и в JSON. Это удобно, потому что некоторые конфиги приходится получать из консоли браузера
## Список конфигов
```dataview
table description as "Описание", config_default_type as "Тип по умолчанию (см. ниже)", config_default_format as "Формат по умолчанию" from "Airflow Providers/Configs"
```

| Название                                 | Описание                                                                                                                     | Тип по умолчанию (см. ниже) | Формат по умолчанию |
| ---------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | --------------------------- | ------------------- |
| [[Airflow Providers/Configs/connectors]] | Коннекторы (докер-образы), которые нужно подключить в Airbyte                                                                | `templated_file`            | `yaml`              |
| [[presets]]                              | Конфигурации Airbyte Source, Destination и Connection, которые используются при создании нового источника                    | `templated_file`            | `json`              |
| datasources                              | Источники данных, на каждый из которых в Airbyte нужно настроить Source и Destination                                        | `datacraft_variable`        | `json`              |
| metadata                                 | Информация о модели данных: список сущностей с атрибутами и связи между ними                                                 | `templated_file`            | `yaml`              |
| events                                   | Правила определений событий, которые используются в атрибуции и воронке                                                      | `templated_file`            | `yaml`              |
| attributions                             | Правила расчета моделей атрибуций                                                                                            | `datacraft_variable`        | `json`              |
| base                                     | Остальные настройки. Отдельные переменные этого конфига могут быть в разных местах за счет отдельных метаконфигов (см. ниже) | `templated_file`            | `yaml`              |
## Типы конфигов
То, откуда функция `get_configs(...)` будет брать конфиг, зависит от его типа и пути. Тип по умолчанию приведен в таблице выше, но пользователь может его поменять (см. [[#Метаконфиги]]). Путь тоже может быть задан в метаконфиге. Типы конфигов бывают следующие:

| Тип                  | Описание                                                                                                                                                                                                                                                                               | Когда применяется                                                                                                                                                                                  | Путь по умолчанию                                                                                                                                                           |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `datacraft_variable` | Значение берется из раздела словаря, который находится в переменной Airflow `from_datacraft{путь}`. Например, для `datasources` по умолчанию `get_configs` возьмет переменную Airflow `from_datacraft`, преобразует JSON в словарь из словаря выберет значение по ключу `datasources`. | Когда нужно пользователю дать возможность установить значение конфига через интерфейс **dataCraft**. Все данные от пользователя **dataCraft** складывает в одну переменную Airflow в виде словаря. | Пустой, что означает, что конфиг будет взят из переменной Airflow `from_datacraft`. Если задать путь `_v2`, то конфиг будет взят из переменной Airflow `from_datacraft_v2`. |
| `other_variable`     | Значение берется из переменной Airflow. Путь определяет название этой переменной. В отличие от предыдущего типа, значение переменной должно лежать на верхнем уровне, а не в определенном ключе словаря.                                                                               | Когда нужно дать возможность быстро изменять значение конфига, но в интерфейсе **dataCraft** задание этого конфига не предусмотрено                                                                | Совпадает с названием конфига                                                                                                                                               |
| `file`               | Значение берется из файла по заданному пути. Путь указывается относительно папки с DAG’ами. Можно указать и абсолютный путь.                                                                                                                                                           | Когда пользователь хочет изменить значение конфига и хранить его под git’ом                                                                                                                        | `configs/{название конфига}`                                                                                                                                                |
| `templated_file`     | Значение берется из файла, лежащего в проекте dbt в папке `configs`. Файл должен называться `{название конфига}{путь}.{формат}`. Если же такого файла нет, то значение берется из                                                                                                      | Когда нужно взять преднастроенное значение конфига из **etlCraft**, при этом дать возможность пользователю перезаписать это значение                                                               | Пустой                                                                                                                                                                      |
## Метаконфиги
Пользователь может поменять тип и формат конфига. Для этого для каждого конфига можно задать в Airflow следующие переменные:
- `source_for_config_{неймспейс}_{название конфига}` — для типа
- `format_for_config_{неймспейс}_{название конфига}` — для формата (`json` или `yaml`)
- `path_for_config_{неймспейс}_{название конфига}` — для пути.

Для конфига `base` в конце можно указывать название переменной, например, `source_for_config_etlcraft_base_airflow_workspace_slug`. Это дает возможность собрать конфиг `base` по частям из разных мест.

## Функция `get_configs(...)`
Функция формирует и возвращает словарь, содержащий выбранные конфиги.
```
import airflow_providers_etlcraft as etlcraft
get_configs(['base', 'presets'], 'myspace')
```
### Аргументы
- `config_list` (по умолчанию `[base]`) — список, содержащий названия конфигов, которые нужно получить
- `namespace` (по умолчанию `etlcraft`) — строка, определяющая, как функция будет искать [[#Метаконфиги]].
### Алгоритм работы
1. Собираем значения всех переменных Airflow, начинающихся на `source/format/path_for_config` для заданного неймспейса и конфигов.
2. Определяем способ получения каждого конфига, а также список переменных конфига `base` (если он есть в списке), которые нужно получать отдельно.
3. Если хоть один конфиг из списка найти указанным способом не удалось — возвращаем ошибку `EtlcraftConfigError`.
4. Возвращаем словарь, в котором ключами являются названия конфигов, например `{"base": {...}, "connectors": {...}`.

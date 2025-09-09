### **Как установить и использовать пакеты airflow_provider_datacraft**

Кастомный провайдер airflow состоит из четырёх пакетов:
-  apache-airflow-providers-datacraft
-  apache-airflow-providers-datacraft-airbyte
-  apache-airflow-providers-datacraft-dags
-  apache-airflow-providers-datacraft-defaults

Все они находятся в папке `dbt-etlcraft/airflow_providers` Первый из них - главный

Предварительно рекомендуется установить пакетный менеджер - pip install hatch и обновить версию pip - `python -m pip install --upgrade pip`


**Установка:**
1. Быстрая установка,  находясь в папке `dbt-etlcraft/airflow_providers/apache-airflow-providers-datacraft` нужно выполнить команду `pip install .[with-datacraft-subpackages]` - это установит все 4 пакета


Если нужно собрать библиотеку(Например чтобы интегрировать пакеты с проектом airflow), нужно в каждой из папок выше запустить команду `hatch build` и в папке `dist` появится файл в формате `.whl`

Модули библиотеки, после установки, вызываются через from airflow.providers.datacraft.xxx

**Запуск тестов:**
Сами тесты находятся в директории `dbt-etlcraft/airflow_providers/apache-airflow-providers-datacraft/etlcraft_tests/providers`

Для их запуска выполняется команда `hatch env run pytest etlcraft_tests/providers в директории `dbt-etlcraft/airflow_providers/apache-airflow-providers-datacraft`

Также, есть тесты для операторов airbyte, но чтобы они работали - нужно заменить `host`, `port`, `login`, `password`, `get_workspace_id` в conftest.py. И запустить команду `hatch env run pytest etlcraft_tests/providers --run-airbyte-tests
Без флага `--run-airbyte-tests` тесты на операторы airbyte будут автоматически пропускаться

В `dbt-etlcraft/airflow_providers/apache-airflow-providers-datacraft/etlcraft_tests/providers/conftest.py` есть функция, с помощью которой можно получить словарь с переменными airflow для тестирования

В `etlcraft_tests/providers/conftest.py` находится код инициализирующий базу данных для тестов, также в этом файле прописываются фикстуры, если нужно их использовать напрямую в тестах, без импорта

________________________________________________________________________

UPD : Я еще раз протестил установку в development режиме, и все-таки я пока не нашел способа, чтобы библиотека работала в editable режиме, как я понял, из-за большой вложенности структуры установленных библиотек airflow и специфичности установки кастомного провайдера в editable-mode (Устанавливаются не файлы пакета, а лишь ссылка на директорию в формате .pth)

Поэтому есть другой способ обновлять пакеты после изменения в них: 
1. (Например) Есть уже установленные 4 пакета: datacraft, datacraft-airbyte, datacraft-dags, datacraft-defaults
2. Внесли какие-то изменения в исходные пакеты
3. Устанавливаем снова библиотеку с внесенными изменениями - 
   `pip install --no-cache-dir .[with-datacraft-subpackages]` в директории `airflow_providers/apache-airflow-providers-datacraft`

Удалить все библиотеки можно с помощью команды:   `pip uninstall apache-airflow-providers-datacraft apache-airflow-providers-datacraft-airbyte apache-airflow-providers-datacraft-dags apache-airflow-providers-datacraft-defaults`

________________________________________________________________________

**Возможные ошибки во время использования:**

Если при запуске тестов в первый раз появляется ошибка, `TypeError: int() argument must be a string, a bytes-like object or a real number, not 'NoneType'`  То запускаем тесты еще раз, эта ошибка из-за отсутствия записей при прогоне тестов впервые.
При последующих запусках этой ошибки не будет

Также может быть ошибка при запуске тестов, когда в логах пишет об отсутствии каких либо миграций(revisions)(Alembic). И она повторяется с каждым запуском - то можно удалить вручную контейнер с БД для тестов
То есть с помощью команды `docker container ls -a` находим контейнер с именем `providers_postgres_tests`, и удаляем его через `docker stop providers_postgres_tests && docker rm providers_postgres_tests` 
И снова запускаем тесты, БД создастся автоматически снова


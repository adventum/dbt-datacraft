# отличие от второй версии - добавляю условия для глобальных registry + добавила более подробную документацию
# в ф-ию transform_json также внесены изменения 

import json
from pathlib import Path
import os
import shutil
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime
import logging
from airflow import XComArg
from airbyte_airflow_provider_advm.operators import CollectConfigsOperator
from airbyte_airflow_provider_advm.utils import etlcraft_variable
from airflow.operators.bash_operator import BashOperator

DBT_DIR = Path(Variable.get('dbt_dir_test'))
models_dir = Path(DBT_DIR) / 'models_test_airflow_NEW_v3'
#пока беру метадату из файла 
#metadata_dir = Path(Variable.get('metadata_dir'))
#stages = Variable.get('stages', deserialize_json=True)
# решили, то луше захардкодить тут 
stages = {
    "1_silos": [
        "normalize",
        "incremental"
    ],
    "2_join": [
        "join"
    ],
    "3_staging": [
        "combine",
        "hash"
    ],
    "4_raw": [
        "link"
    ],
    "5_graph": [
        "tuples",
        "lookup",
        "unique",
        "edge",
        "glue",
        "qid"
    ],
    "6_full": [
        "full"
    ],
    "7_attr": [
        "prepare_with_qid",
        "create_events",
        "add_row_number",
        "find_new_period",
        "calculate_period_number",
        "create_missed_steps",
        "join_to_attr_prepare_with_qid",
        "model",
        "final_table"
    ],
    "8_dataset": [
        "dataset"
    ]
}


@task(task_id="clear_models_dir", doc_md=f"""**Описание задачи:** 
Эта задача удаляет из папки `models` все файлы и папки, кроме папки `manual` и её содержимого.
Это необходимо для того, чтобы файлы моделей оставались актуальными, и в проекте не оставалось моделей для неактивных источников данных.

**Входные данные:** Путь к папке `models`.
**Выходные данные:** Очищенная папка `models`, за исключением папки `manual` и её содержимого, если она существует.""")
def clear_models_dir(path, self_delete=False):
    logger = logging.getLogger("airflow.task")
    
    # Проверяем, существует ли папка
    if not os.path.exists(path):
        logging.error(f"The directory '{path}' does not exist.")
        return "Работа завершена: директория не существует"
    
    if not os.listdir(path):
        # Если основная директория пуста, логируем это и завершаем выполнение
        logger.info(f"The directory {path} was processed but nothing was deleted because it is empty.")
        return "Работа завершена: директория пуста"

    def delete_content(directory):
        # Обход директорий с topdown=True для фильтрации директорий до их обработки
        for root, dirs, files in os.walk(directory, topdown=True):
        # Исключаем директории 'manual' из удаления
            dirs[:] = [d for d in dirs if d != 'manual']

            # Удаляем файлы
            for name in files:
                file_path = os.path.join(root, name)
                os.remove(file_path)
                logger.info(f"Removed file {file_path}")

        # Рекурсивное удаление пустых директорий, кроме тех, что содержат 'manual'
        for root, dirs, _ in os.walk(directory, topdown=False):
            for name in dirs:
                dir_path = os.path.join(root, name)
                # Удаляем директорию, если она пуста и не является 'manual'
                if not os.listdir(dir_path) and name != 'manual':
                    shutil.rmtree(dir_path)
                    logger.info(f"Removed directory {dir_path}")
                elif name == 'manual':
                    logger.info(f"Directory 'manual' kept: {dir_path}")

    try:
        delete_content(path)

        # Удаляем корневую директорию, если она пуста и нужно её удалить
        if self_delete and os.path.exists(path) and not os.listdir(path):
            shutil.rmtree(path)
            logger.info(f"Removed root directory {path}")

    except Exception as e:
        logger.error(f"Error processing path {path}: {e}")

    return "Работа завершена"


@task(task_id="prepare_input_data_for_create_models_task", 
      doc_md=f"""**Описание задачи:** 
Эта задача готовит входные данные в нужном формате для функции по созданию файлов моделей dbt.

**Входные данные:** Основная директория проекта, конфигурации, собранные с помощью CollectConfigsOperator.
**Выходные данные:** Подготовленные данные для создания моделей, готовые для передачи в функцию `create_models`.

Внутри задачи используются следующие функции:

- **collect_files_in_manual_folders:** Собирает список файлов моделей, созданных вручную в папках `manual`. Эти файлы исключаются из автоматического создания моделей. Если вручную созданные модели были найдены, их имена добавляются в список, а также выводятся в лог. если "ручных" моделей найдено не было, то в лог выводится сообщение: "Файлов с ручными моделями обнаружено не было."

- **get_registry_links_from_metadata:** Извлекает информацию о линках с pipeline `registry` из метадаты. Эти данные затем используются в функции `transform_json` для сопоставления линков из `presets.json`. Если совпадение найдено, функция `transform_json` добавляет поле `"registry_link"` с именем линка, иначе `"registry_link": None`.  

- **transform_json:** Эта функция сопоставляет данные из переменной from_datacraft об активных источниках и данных из конфига presets и формирует JSON структуру, в которой данные сгруппированы по пайплайнам. Помимо этого, как было указано выше, функция сопоставляет данные с даннми метадат и добавляет в каждый пресет поле `registry_link`, если линк соответствует критериям.

- **get_pipeline_list_for_datasets(datacraft_data, source_presets):** Извлекает и возвращает список pipeline для каждого dataset на основе данных из переменной from_datacraft и presets.  

- **get_attr_and_dataset_data(datacraft_data):** Возвращает данные о моделях атрибуции и датасетах из переменной from_datacraft. Эти данные потом используются для генерации файлов моделей на слоях 7_attr и 8_dataset.

Также есть 3 функции, которые уже непосредственно формируют данные необходимые для создания файлов.

- **generate_model_files(stage, sublayer, data):** создаёт названия файлов, учитывая, что на каждом слое(stage) и подслое (sublayer) свои шаблоны для названия файла.
- **generate_path(stage, sublayer, data):** Генерирует путь для сохранения модели в зависимости от слое (stage), подслоя (sublayer) и данных.
- **generate_file_content(stage, sublayer, data, files_list=None):** Создаёт содержимое файла модели в зависимости от этапа, подслоя и данных. 
  
После вызова вспомогательных функций, задача проходит по всем слоям и подслоям, получает входные данные из результата работы функций transform_json и get_attr_and_dataset_data и генерирует файлы моделей
с помощью функций generate_filename, generate_path и generate_file_content.

**Результатом работы** функции prepare_input_data_for_create_models_func является словарь следующего вида:
```python
{{
    '1_silos': {{
        'normalize': [
            {{
                'file_name': 'normalize_appmetrica_events_default_events.sql',
                'path': '/mnt/c/test/models_test_airflow_NEW_v4/1_silos/1_normalize',
                'content': '{{% raw %}}{{{{ etlcraft.normalize() }}}}{{% endraw %}}'
            }},
            {{
                'file_name': 'normalize_appmetrica_events_default_deeplinks.sql',
                'path': '/mnt/c/test/models_test_airflow_NEW_v4/1_silos/1_normalize',
                'content': '{{% raw %}}{{{{ etlcraft.normalize() }}}}{{% endraw %}}'
            }},
            {{
                'file_name': 'normalize_appmetrica_events_default_installations.sql',
                'path': '/mnt/c/test/models_test_airflow_NEW_v4/1_silos/1_normalize',
                'content': '{{% raw %}}{{{{ etlcraft.normalize() }}}}{{% endraw %}}'
            }}
        ],
        'incremental': [
            {{
                'file_name': 'incremental_appmetrica_events_default_events.sql',
                'path': '/mnt/c/test/models_test_airflow_NEW_v4/1_silos/2_incremental',
                'content': "{{% raw %}}-- depends_on: {{{{ ref('normalize_appmetrica_events_default_events') }}}}{{% endraw %}}\n{{% raw %}}{{{{ etlcraft.incremental() }}}}{{% endraw %}}"
            }},
            {{
                'file_name': 'incremental_appmetrica_events_default_deeplinks.sql',
                'path': '/mnt/c/test/models_test_airflow_NEW_v4/1_silos/2_incremental',
                'content': "{{% raw %}}-- depends_on: {{{{ ref('normalize_appmetrica_events_default_deeplinks') }}}}{{% endraw %}}\n{{% raw %}}{{{{ etlcraft.incremental() }}}}{{% endraw %}}"
            }},
            {{
                'file_name': 'incremental_appmetrica_events_default_installations.sql',
                'path': '/mnt/c/test/models_test_airflow_NEW_v4/1_silos/2_incremental',
                'content': "{{% raw %}}-- depends_on: {{{{ ref('normalize_appmetrica_events_default_installations') }}}}{{% endraw %}}\n{{% raw %}}{{{{ etlcraft.incremental() }}}}{{% endraw %}}"
            }}
        ]
    }},
    '2_join': {{
        # Данные для '2_join'
    }}
    # Данные для следующих слоёв
}}```
""")
def prepare_input_data_for_create_models_func(main_directory, collect_configs):
    
    source_presets = collect_configs['presets']["source_presets"]
    datacraft_data = collect_configs['from_datacraft']
    metadata = collect_configs['metadata']

    #эта функция собирает список файлов моделей, созданных в ручную, если таковые имеются.
    #это нужно для того, чтобы исключить из автоматического создания эти модели
    def collect_files_in_manual_folders(main_directory):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        file_list = []

        for root, dirs, files in os.walk(main_directory):
            for dir in dirs:
                if dir == "manual":
                    manual_path = os.path.join(root, dir)

                    for root_manual, _, files_manual in os.walk(manual_path):
                        for file in files_manual:
                            file_list.append(os.path.join(file))

        if file_list:
            logger.info("Найдены следующие файлы в папках manual:")
            for file in file_list:
                logger.info(file)
        else:
            logger.info("Файлов с ручными моделями обнаружено не было.")

        return file_list
    
    # Функция для извлечения информации о линках из metadata_1.sq
    # нужна, чтобы найти те линки, у которых pipeline registry 
    # пример значения, которое возвращает {'UtmHashRegistry': 'registry', 'AppProfileMatching': 'registry'}
    # эти данне дальше используются в ф-ии transform_json, чтобы каждому споставить список линков из presets.json 
    # и если совпадение есть то в результат работ ф-ии transform_json добавляется поле "registry_link": "название линка"
    # если совпадения нет, то "registry_link": None
    def get_registry_links_from_metadata(metadata):
        links = {}
        
        # Извлекаем секцию "links" из переданного метаданных
        links_metadata = metadata.get('links', {})
        
        # Проходим по всем линкам в метаданных
        for link_name, link_info in links_metadata.items():
            # Если pipeline для данного линка равен "registry"
            if link_info.get('pipeline') == 'registry':
                links[link_name] = 'registry'
        
        return links

    # Получаем данные по линкам с пайплайном registry из метадаты
    metadata_registry_links = get_registry_links_from_metadata(metadata)
    print("Линки с пайплайном registry: ", metadata_registry_links)
    
    #эта функция объединяет информаию из переменной from_datacraft (содержит данных об активных источниках) с данными
    # из шаблона персетов presets.json 
    # (предварительно все нужные данные объединяются в collect_configs с помощью оператора CollectConfigsOperator)
    # вопрос/предложение - тут возможно нужно добавить группировку по проектам ?
    def transform_json(datacraft_data, source_presets, metadata_registry_links):
        transformed_json = {}
        #source_presets = presets["source_presets"]

        preset_names = set()
        for datasource in datacraft_data["datasources"].values():
            preset_names.add(datasource["preset"])

        for preset_name, preset_data in source_presets.items():
            if preset_name not in preset_names:
                continue

            source_type = preset_name.split('_')[0]
            entities = preset_data["entities"]
            links = preset_data["links"]
            preset_name = preset_name.split('_')[1]
            streams = preset_data["syncCatalog"]["streams"]

            # Ищем совпадения с линками из metadata_1.sql
            registry_link = None
            for link in links:
                if link in metadata_registry_links:
                    registry_link = link
                    break

            for stream in streams:
                stream_name = stream["stream"]["name"]
                pipeline_type = stream["stream"]["pipeline"]
                fields = list(stream["stream"]["jsonSchema"]["properties"].keys())

                if pipeline_type not in transformed_json:
                    transformed_json[pipeline_type] = {
                        "source_types": {}
                    }

                if source_type not in transformed_json[pipeline_type]["source_types"]:
                    transformed_json[pipeline_type]["source_types"][source_type] = {
                        "presets": {}
                    }

                if preset_name not in transformed_json[pipeline_type]["source_types"][source_type]["presets"]:
                    transformed_json[pipeline_type]["source_types"][source_type]["presets"][preset_name] = {
                        "entities": entities,
                        "links": links,
                        "registry_link": registry_link,
                        "streams": {}
                    }

                transformed_json[pipeline_type]["source_types"][source_type]["presets"][preset_name]["streams"][stream_name] = {
                    "fields": fields
                }

        return transformed_json
    
    # функция сотставляет список пайплайнов для каждого dataset в разделе datasets, 
    # нужно для depens on при создании моделей на шаге 8_dataset
    # в список не добавляем пайплайн registry, так как он не нужен, full таблиц registry не может быть
    # ЛОГИКА ТАКАЯ: мы смотрим id источников, указанных в подразделе 'sources' в разделе 'datasets'. 
        # В разделе datasources находим источники с этим id, смотрим id пресетов, 
        # идём в раздел пресетов и выбираем уникальные названия пайплайнов (пайпланы указаны для каждого стрима)
        # получаем на выходе такого типа json: 
        # {'event_table': ['periodstat', 'periodstat222', 'datestat'], 'cost_table': ['periodstat222', 'datestat']}
        # где event_table и cost_table - названия датасетов, указанных в разделе datasets 
    def get_pipeline_list_for_datasets(datacraft_data, source_presets):
        result = {}

        # Extract the necessary data
        datasources = datacraft_data.get('datasources', {})
        datasets = datacraft_data.get('datasets', {})
        presets = source_presets 
        

        # Iterate through each dataset
        for dataset_name, dataset_info in datasets.items():
            pipeline_set = set()
            source_ids = dataset_info.get('sources', [])

            # For each source id, find the corresponding preset and collect pipeline names
            for source_id in source_ids:
                source_info = datasources.get(source_id, {})
                preset_id = source_info.get('preset')
                preset_info = presets.get(preset_id, {})
                streams = preset_info.get('syncCatalog', {}).get('streams', [])

                # Collect unique pipeline names from the streams
                for stream in streams:
                    pipeline_name = stream.get('stream', {}).get('pipeline')
                    if pipeline_name and pipeline_name != 'registry':
                        pipeline_set.add(pipeline_name)

            # Store the unique pipeline names for the current dataset
            result[dataset_name] = list(pipeline_set)

        return result
    
    #отдельно получаем данные по моделям атрибуии и датасетам
    def get_attr_and_dataset_data(datacraft_data):
        #datacraft_data = collect_configs['from_datacraft']
        attr_and_dataset_data = {}
        if "attribution_models" in datacraft_data:
            attr_and_dataset_data["attribution_models"] = datacraft_data["attribution_models"]
        if "datasets" in datacraft_data:
            attr_and_dataset_data["datasets"] = datacraft_data["datasets"]
        return attr_and_dataset_data
    
    #теперь три ф-ии для создания непосредственно входной информаии 

    #1) формирует названия файлов 
    def generate_filename(stage, sublayer, data):
        if stage == "1_silos":
            return f"{sublayer}_{data['source_type']}_{data['pipeline']}_{data['preset']}_{data['stream']}.sql"
        elif stage == "2_join":
            if data['pipeline'] != 'registry':
                return f"join_{data['source_type']}_{data['pipeline']}.sql"
            elif data['pipeline'] == 'registry' and data['registry_link'] is not None:
                return f"join_{data['source_type']}_{data['pipeline']}_{data['registry_link'].lower()}.sql"
        elif stage == "3_staging":
            if data['pipeline'] != 'registry':
                return f"{sublayer}_{data['pipeline']}.sql"
            elif data['pipeline'] == 'registry' and data['registry_link'] is not None:
                return f"{sublayer}_{data['pipeline']}_{data['registry_link'].lower()}.sql"
        elif stage == "4_raw":
            if data['pipeline'] != 'registry':
                return f"link_{data['pipeline']}.sql"
            elif data['pipeline'] == 'registry' and data['registry_link'] is not None:
                return f"link_{data['pipeline']}_{data['registry_link'].lower()}.sql"
        elif stage == "5_graph" and 'events' in data['pipeline_names']:
            return f"graph_{sublayer}.sql"
        elif stage == "6_full" and data['pipeline'] != 'registry':
            return f"full_{data['pipeline']}.sql"
        elif stage == "7_attr" and 'attribution_models' in attr_and_dataset_data.keys() and 'events' in data['pipeline_names']:
            return f"attr_{data['model_name']}_{sublayer}.sql"
        elif stage == "8_dataset" and 'datasets' in attr_and_dataset_data.keys():
            return f"dataset_{data['dataset_name']}.sql"
        return None

    #2) формирует путь к файлу
    def generate_path(stage, sublayer, data):
        if stage == "1_silos":
            return f"{models_dir}/1_silos/{data['idx'] + 1}_{sublayer}"
        elif stage == "2_join":
            return f"{models_dir}/2_join/{data['pipeline']}"
        elif stage == "3_staging":
            return f"{models_dir}/3_staging/{data['idx'] + 1}_{sublayer}"
        elif stage == "4_raw": # and data['pipeline'] != 'registry':
            return f"{models_dir}/4_raw"
        elif stage == "5_graph" and 'events' in data['pipeline_names']:
            return f"{models_dir}/5_graph/{data['idx'] + 1}_{sublayer}"
        elif stage == "6_full":
            return f"{models_dir}/6_full"
        elif stage == "7_attr" and 'attribution_models' in attr_and_dataset_data.keys() and 'events' in data['pipeline_names']:
            return f"{models_dir}/7_attr/{data['idx'] + 1}_{sublayer}"
        elif stage == "8_dataset" and 'datasets' in attr_and_dataset_data.keys():
            return f"{models_dir}/8_dataset"
        return None

    #3) формирует содержимое файла (без depends on)
    #def generate_file_content(stage, sublayer):
        #if stage in ["1_silos", "3_staging", "4_raw"]:
            #return f"{{{{ etlcraft.{sublayer}() }}}}"
            #return f"{{% raw %}}{{{{ etlcraft.{sublayer}() }}}}{{% endraw %}}"
        #else:
            #return f"{{{{ etlcraft.{stage.split('_')[1]}() }}}}"
            #return f"{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"
    
    #3)Формируем содержимое файла с добавлением depends on 
    def generate_file_content(stage, sublayer, data, files_list=None):
        if stage == "1_silos":
            if sublayer == "normalize": 
            #return f"{{{{ etlcraft.{sublayer}() }}}}"
                return f"{{% raw %}}{{{{ etlcraft.{sublayer}() }}}}{{% endraw %}}"
            else:
                return f"""{{% raw %}}-- depends_on: {{{{ ref('normalize_{data['source_type']}_{data['pipeline']}_{data['preset']}_{data['stream']}') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{sublayer}() }}}}{{% endraw %}}"""
        
        elif stage == "2_join":
            # Инициализируем пустую строку для сборки рефов
            refs_string = ""
            # Проверка, что files_list и source_type не пустые
            if files_list and data['source_type'] in files_list:
            # Итерация по списку файлов для конкретного source_type
                for incremental_file_name in files_list[data['source_type']]:
                    refs_string += f"{{% raw %}}-- depends_on: {{{{ ref('{incremental_file_name}') }}}}{{% endraw %}}\n"
            # Возвращаем результирующую строку
            return f"""{refs_string}{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
        
        elif stage == "3_staging":
            if sublayer == "combine":
                # Инициализируем пустую строку для сборки рефов
                refs_string = ""
                # Проверка, что files_list и pipeline не пустые
                if files_list and data['pipeline'] in files_list:
                    # Итерация по списку файлов для конкретного pipeline
                    for join_file_name in files_list[data['pipeline']]:
                        refs_string += f"{{% raw %}}-- depends_on: {{{{ ref('{join_file_name}') }}}}{{% endraw %}}\n"
                # Возвращаем результирующую строку
                return f"""{refs_string}{{% raw %}}{{{{ etlcraft.{sublayer}() }}}}{{% endraw %}}"""
            # для hash    
            else:
                if data['pipeline'] != 'registry':
                    return f"""{{% raw %}}-- depends_on: {{{{ ref('combine_{data['pipeline']}') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{sublayer}() }}}}{{% endraw %}}"""
                elif data['pipeline'] == 'registry' and file_data['registry_link'] is not None:
                    return f"""{{% raw %}}-- depends_on: {{{{ ref('combine_{data['pipeline']}_{data['registry_link'].lower()}') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{sublayer}() }}}}{{% endraw %}}"""
                
        
        elif stage == "4_raw":
            if data['pipeline'] != 'registry':
                return f"""{{% raw %}}-- depends_on: {{{{ ref('hash_{data['pipeline']}') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{sublayer}() }}}}{{% endraw %}}"""
            elif data['pipeline'] == 'registry' and file_data['registry_link'] is not None:
                return f"""{{% raw %}}-- depends_on: {{{{ ref('hash_{data['pipeline']}_{data['registry_link'].lower()}') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{sublayer}() }}}}{{% endraw %}}"""
        
        elif stage == "5_graph":
            if sublayer == "tuples":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('link_events') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "lookup":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('graph_tuples') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "unique":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('graph_lookup') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "edge":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('graph_unique') }}}}{{% endraw %}}
{{% raw %}}-- depends_on: {{{{ ref('graph_tuples') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "glue":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('graph_edge') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""          
            elif sublayer == "qid":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('graph_lookup') }}}}{{% endraw %}}
{{% raw %}}-- depends_on: {{{{ ref('graph_glue') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
                   
        elif stage == "6_full": #во все full добавляем все link_registry файлы
            if data['pipeline'] == "events":
                # Инициализируем пустую строку для сборки рефов
                refs_string = ""
                for file_name in files_list:
                        refs_string += f"{{% raw %}}-- depends_on: {{{{ ref('{file_name}') }}}}{{% endraw %}}\n"
                # Возвращаем результирующую строку
                return f"""{refs_string}{{% raw %}}-- depends_on: {{{{ ref('graph_qid') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            else:
                # Инициализируем пустую строку для сборки рефов
                refs_string = ""
                for file_name in files_list:
                        refs_string += f"{{% raw %}}-- depends_on: {{{{ ref('{file_name}') }}}}{{% endraw %}}\n"
                # Возвращаем результирующую строку
                return f"""{refs_string}{{% raw %}}-- depends_on: {{{{ ref('link_{data['pipeline']}') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
        
        elif stage == "7_attr":
            if sublayer == "prepare_with_qid":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('full_events') }}}}{{% endraw %}}
{{% raw %}}-- depends_on: {{{{ ref('graph_qid') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "create_events":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_prepare_with_qid') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "add_row_number":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_create_events') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "find_new_period":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_add_row_number') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "calculate_period_number":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_find_new_period') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "create_missed_steps":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_calculate_period_number') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "join_to_attr_prepare_with_qid":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_prepare_with_qid') }}}}{{% endraw %}}
{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_create_missed_steps') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "model":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_join_to_attr_prepare_with_qid') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            elif sublayer == "final_table":
                return f"""{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_join_to_attr_prepare_with_qid') }}}}{{% endraw %}}
{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_model') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
        
        elif stage == "8_dataset":
            if data['model_name'] == None: 
                # Инициализируем пустую строку для сборки рефов
                refs_string = ""
                # Итерация по списку пайплайнов
                for pipeline in data['pipelines_list_for_dataset']:
                    refs_string += f"{{% raw %}}-- depends_on: {{{{ ref('full_{pipeline}') }}}}{{% endraw %}}\n"
                # Возвращаем результирующую строку
                return f"""{refs_string}{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
            else:
                refs_string = ""
                # Итерация по списку пайплайнов
                for pipeline in data['pipelines_list_for_dataset']:
                    refs_string += f"{{% raw %}}-- depends_on: {{{{ ref('full_{pipeline}') }}}}{{% endraw %}}\n"
                # Возвращаем результирующую строку
                return f"""{refs_string}{{% raw %}}-- depends_on: {{{{ ref('attr_{data['model_name']}_final_table') }}}}{{% endraw %}}
{{% raw %}}{{{{ etlcraft.{stage.split('_')[1]}() }}}}{{% endraw %}}"""
    ###############   коне ф-ии  generate_file_content   ############################
    
    #теперь вызываем подготовительные ф-ии    
    data = transform_json(datacraft_data, source_presets, metadata_registry_links)
    attr_and_dataset_data = get_attr_and_dataset_data(datacraft_data)
    files_in_manual_folders = collect_files_in_manual_folders(models_dir)

    #получам данные о слоях
    stages = collect_configs["stages"] # ТУТ НУЖНО БУДЕТ УПРОСТИТЬ просто взять из переменной stages
    
    
    
    # Словарь для хранения названий файлов подслоя incremental
    incremental_files = {}
    # Словарь для хранения названий файлов слоя join
    join_files = {}
    # Список для хранения названий файлов link_registry_{link_name} (с глобальными registry), нужно для depends on на слое full
    link_registry_files = []

    input_for_creat_models = {}
    #теперь формируем название файлов, пути и содержимое 
    for stage, sublayers in stages.items():
        if stage == "1_silos":
            input_for_creat_models[stage] = {}
            for idx, sublayer in enumerate(sublayers):
                input_for_creat_models[stage][sublayer] = []
                for pipeline, pipeline_data in data.items():
                    for source_type, source_data in pipeline_data['source_types'].items():
                        for preset, preset_data in source_data['presets'].items():
                            for stream in preset_data['streams']:
                                file_data = {
                                            'pipeline': pipeline,
                                            'source_type': source_type,
                                            'preset': preset,
                                            'stream': stream,
                                            'idx': idx
                                        }
                                file_name = generate_filename(stage, sublayer, file_data)
                                path = generate_path(stage, sublayer, file_data)
                                content = generate_file_content(stage, sublayer, file_data)
                                #тут проверка: 
                                if file_name and file_name not in files_in_manual_folders: 
                                    # если ок, то добавляем в input_for_creat_models 
                                    input_for_creat_models[stage][sublayer].append({
                                    "file_name": file_name,
                                    "path": path,
                                    "content": content
                                        })
                                
                                # Проверка, если подслой incremental, собираем названия файлов
                                if sublayer == "incremental":
                                    # Удаляем расширение .sql
                                    file_name_without_extension = file_name.replace('.sql', '')
                                
                                # Добавляем название файла в словарь incremental_files по source_type
                                    if source_type not in incremental_files:
                                        incremental_files[source_type] = []
                                
                                    incremental_files[source_type].append(file_name_without_extension)

        elif stage == "2_join":
            input_for_creat_models[stage] = {}
            for idx, sublayer in enumerate(sublayers):
                input_for_creat_models[stage][sublayer] = []
                for pipeline, pipeline_data in data.items():
                    for source_type, source_data in pipeline_data['source_types'].items():
                        registry_links = {
                            preset_name: preset_data.get('registry_link')
                            for preset_name, preset_data in source_data['presets'].items()
                        }
                        print(f"registry_links на {stage} и {sublayer} для пайплайна {pipeline} и источника {source_type}: {registry_links}")

                        for preset_name, registry_link in registry_links.items():
                            file_data = {
                                'pipeline': pipeline,
                                'source_type': source_type,
                                'registry_link': registry_link
                            }

                            if file_data['pipeline'] != 'registry' or (file_data['pipeline'] == 'registry' and file_data['registry_link'] is not None):
                                file_name = generate_filename(stage, sublayer, file_data)
                                path = generate_path(stage, sublayer, file_data)
                                content = generate_file_content(stage, sublayer, file_data, incremental_files)

                                if file_name and file_name not in files_in_manual_folders: 
                                    input_for_creat_models[stage][sublayer].append({
                                        "file_name": file_name,
                                        "path": path,
                                        "content": content
                                    })

                                if pipeline not in join_files:
                                    join_files[pipeline] = []

                                # Удаляем расширение .sql и проверяем на уникальность перед добавлением
                                file_name_without_extension = file_name.replace('.sql', '')
                                if file_name_without_extension not in join_files[pipeline]:
                                    join_files[pipeline].append(file_name_without_extension)
                           
            print("Список join-файлов по пайплайнам: ", join_files)
                                    

        elif stage in ["3_staging", "4_raw"]:
            input_for_creat_models[stage] = {}
            
            # Список для отслеживания уже созданных файлов (так как в data.items() мб несколько одинаковх пайплайнов (у разных истоников) и создавался дважды файл)
            # далее будем проверять есть ли файл в этом списке прежде чем добавлять его в итоговый вывод 
            created_files = set()
            
            for idx, sublayer in enumerate(sublayers):
                input_for_creat_models[stage][sublayer] = []
                for pipeline, pipeline_data in data.items():
                    # Собираем registry_links (для каждого пайплайна свой список, у одного пайплайна мб несколько пресетов, а у пресетов могут отличаться линки)
                    #в этом случае, например, если у одного пайплайна несколько пресетов, то registry_links может выглядить так:
                    #registry_links = {
                                        #'default': ['AppProfileMatching', None],
                                        #'another_preset': ['SomeLink']
                                    #}
                    # Инициализируем пустой словарь для хранения registry_links
                    registry_links = {}

                    # Итерируем по pipeline
                    for source_type, source_data in pipeline_data['source_types'].items():
                        for preset_name, preset_data in source_data['presets'].items():
                            # Проверяем, есть ли уже такой preset_name в словаре
                            if preset_name not in registry_links:
                                registry_links[preset_name] = []
        
                            # Добавляем в список значение registry_link (или None)
                            registry_links[preset_name].append(preset_data.get('registry_link'))
                    
                    #print(f"Обрабатываемый pipeline_data для {stage} и {sublayer}: {pipeline_data}")
                    print(f"registry_links на {stage} и {sublayer} для пайплайна {pipeline}: {registry_links}")
                    
                    # Итерация по каждому registry_link (тут итерация отличается от слоя join, т.к у нас список линков для каждого пресета (пример выше))
                    for preset_name, registry_links_list in registry_links.items():
                        for registry_link in registry_links_list:
                            file_data = {
                                'pipeline': pipeline,
                                'idx': idx,
                                'registry_link': registry_link
                            }
                        
                            if file_data['pipeline'] != 'registry' or (file_data['pipeline'] == 'registry' and file_data['registry_link'] is not None):
                                if sublayer == "combine":
                                    file_name = generate_filename(stage, sublayer, file_data)
                                    path = generate_path(stage, sublayer, file_data)
                                    content = generate_file_content(stage, sublayer, file_data, join_files)
                                else:
                                    file_name = generate_filename(stage, sublayer, file_data)
                                    path = generate_path(stage, sublayer, file_data)
                                    content = generate_file_content(stage, sublayer, file_data)

                            # Проверяем, был ли уже создан файл с таким именем
                            if file_name and file_name not in files_in_manual_folders and file_name not in created_files:
                                # Если файл не существует, добавляем его в input_for_creat_models и помечаем как созданный
                                input_for_creat_models[stage][sublayer].append({
                                    "file_name": file_name,
                                    "path": path,
                                    "content": content
                                })
                                created_files.add(file_name)

                                
                                if sublayer == "link" and (file_data['pipeline'] == 'registry' and file_data['registry_link'] is not None):
                                    if file_name not in link_registry_files:
                                        file_name_without_extension = file_name.replace('.sql', '')
                                        link_registry_files.append(file_name_without_extension)

            print("Список link-файлов с глобальнми registry - link_registry_files: ", link_registry_files)
                          

        elif stage == "5_graph":
            #input_for_creat_models[stage] = {}
            stage_data = {}
            pipeline_names = []
            for pipeline, pipeline_data in data.items(): #тут подумать нужно ли так или нет 
                if pipeline not in pipeline_data:
                    pipeline_names.append(pipeline)   
            for idx, sublayer in enumerate(sublayers):
                #input_for_creat_models[stage][sublayer] = []
                sublayer_data = []
                file_data = {'pipeline_names': pipeline_names,
                                'idx': idx } 
                file_name = generate_filename(stage, sublayer, file_data)
                path = generate_path(stage, sublayer, file_data)
                content = generate_file_content(stage, sublayer, file_data)
                #тут проверка: 
                if file_name and file_name not in files_in_manual_folders:
                    # если ок, то добавляем в input_for_creat_models 
                    sublayer_data.append({
                                    "file_name": file_name,
                                    "path": path,
                                    "content": content
                                        })
                #так как графовая склейка проводится только если данные относятся к пайплайну event, то тоже проводим доп проверку, прежде чем добавить данные в input_for_creat_models
                if sublayer_data:
                    stage_data[sublayer] = sublayer_data
            if stage_data:
                input_for_creat_models[stage] = stage_data

        elif stage == "6_full":
            input_for_creat_models[stage] = {}
            for idx, sublayer in enumerate(sublayers):
                input_for_creat_models[stage][sublayer] = []
                for pipeline, pipeline_data in data.items(): 
                    file_data = {'pipeline': pipeline}
                    if file_data['pipeline'] != 'registry':
                        file_name = generate_filename(stage, sublayer, file_data)
                        path = generate_path(stage, sublayer, file_data)
                        content = generate_file_content(stage, sublayer, file_data, link_registry_files)
                        #тут проверка: 
                        if file_name and file_name not in files_in_manual_folders:
                            # если ок, то добавляем в input_for_creat_models
                            input_for_creat_models[stage][sublayer].append({
                                    "file_name": file_name,
                                    "path": path,
                                    "content": content
                                        }) 

        elif stage == "7_attr":
            #input_for_creat_models[stage] = {}
            stage_data = {}
            for idx, sublayer in enumerate(sublayers):
                #input_for_creat_models[stage][sublayer] = []
                sublayer_data = []
                pipeline_names = []
                for pipeline, pipeline_data in data.items(): #тут подумать нужно ли так или нет 
                    if pipeline not in pipeline_data:
                        pipeline_names.append(pipeline)
                for model_name in attr_and_dataset_data.get('attribution_models', {}).keys():
                    file_data = {'model_name': model_name,
                                        'pipeline_names': pipeline_names,
                                        'idx': idx }
                    file_name = generate_filename(stage, sublayer, file_data)
                    path = generate_path(stage, sublayer, file_data)
                    content = generate_file_content(stage, sublayer, file_data)
                    #тут проверка: 
                    if file_name and file_name not in files_in_manual_folders:
                        # если ок, то добавляем в input_for_creat_models
                        sublayer_data.append({
                                    "file_name": file_name,
                                    "path": path,
                                    "content": content
                                        }) 
                #так как не всегда есть данные по моделям атрибуции, то тут, прежде чем добавлять что-то input_for_creat_models, 
                # проверяем не пустой ли список 
                if sublayer_data:
                    stage_data[sublayer] = sublayer_data
            if stage_data:
                input_for_creat_models[stage] = stage_data

        elif stage == "8_dataset":
            pipelines_lists = get_pipeline_list_for_datasets(datacraft_data, source_presets) #получаем список пайпланов для каждого датасета
            print("8_dataset, список пайплайнов для каждого датасета:", pipelines_lists)
            #input_for_creat_models[stage] = {}
            stage_data = {}
            for idx, sublayer in enumerate(sublayers):
                #input_for_creat_models[stage][sublayer] = []
                sublayer_data = []
            #for dataset_name in attr_and_dataset_data.get('datasets', {}).keys():
                for dataset_name, dataset_data in attr_and_dataset_data['datasets'].items():
                    pipelines_list_for_dataset = pipelines_lists[dataset_name]
                    # Получаем значение attr_model или устанавливаем None, если значение отсутствует или пустое
                    attr_model = dataset_data.get('attr_model') or None
                    file_data = {'dataset_name': dataset_name,
                                'model_name': attr_model,
                                'pipelines_list_for_dataset': pipelines_list_for_dataset}
                    file_name = generate_filename(stage, sublayer, file_data)
                    path = generate_path(stage, sublayer, file_data)
                    content = generate_file_content(stage, sublayer, file_data)
                    #тут проверка: 
                    if file_name and file_name not in files_in_manual_folders:
                        # если ок, то добавляем в input_for_creat_models
                        sublayer_data.append({
                                        "file_name": file_name,
                                        "path": path,
                                        "content": content
                                            })
             #аналогично, не всегда могут быть заданы датасеты, так что проверяем 
                if sublayer_data:
                     stage_data[sublayer] = sublayer_data
            if stage_data:
                 input_for_creat_models[stage] = stage_data
    
    return input_for_creat_models


with DAG(
    dag_id="etlcraft_prepare_NEW_v3_1", 
    description="Даг для полготовки к запуску dbt моделей",
    schedule_interval=None,
    tags=["my_dag"]
) as dag:
    
    dag.doc_md = """
### Для чего нужен этот даг?
Основная цель этого дага - автоматезированное создание файлов dbt моделей. 

DAG выполняет несколько задач:

1. **Сбор конфигураций:** С помощью оператора `CollectConfigsOperator` DAG собирает все необходимые входные данные в единый словарь.  
В первую очередь это данные из dataCraft (переменная from_datacraft, содержащая данные об активных источниках, моделях атрибуции и датасетах) и репозитория etlcraft (в репозитории, в папке configs_template, хранятся данные о пресетах (файл presets.json), базовой метадате (файл metadata_basic.yaml) и других необходимых конфигурациях). 

2. **Очистка основной директории:** Задача `clear_models_dir` удаляет из папки `models` все файлы и папки, кроме папки `manual` и её содержимого, если она есть. Это гарантирует, что в проекте останутся только актуальные модели (для активных источников данных и существующих пресетов).

3. **Подготовка входных данных для создания моделей:** Задача `prepare_input_data_for_create_models_task` подготавливает входные данные для функции `create_models`, конвертируя их в нужный формат.

4. **Создание моделей:** На завершающем этапе DAG создаёт файлы моделей с помощью подготовленных данных.

DAG обеспечивает, чтобы проект оставался актуальным и включал только необходимые данные.

**Подготовительная работа перед запуском дага:**

В разделе Variables необходимо указать:
    - путь, по которому расположена папка models. Переменной, содержащей путь к папке, лучше давать стандартное название dbt_dir.  
    - формат (etlcraft_format_for_config_{название конфига}), путь (etlcraft_path_for_config_{название конфига}) и тип конфига* (etlcraft_source_for_config_{название конфига}). *сущесвует 3 основных типа конфигов: file, datacraft_variable, othe_variable. 
"""
    
    collect_configs = CollectConfigsOperator(
        task_id="collect_configs",
        doc_md ="""**Описание задачи:** 

Эта задача собирает в один словарь все входные данные, которые будут использованы на последующих этапах выполнения DAG.

**Конфигурация задачи:**

- **config_names:** Здесь указывается список всех конфигураций, которые нужно включить в итоговый словарь. Например, `'from_datacraft'`, `'stages'`, `'presets'`, `'metadata'`.

- **entire_datacraft_variable:** Если этот параметр установлен в `True`, оператор будет извлекать все данные из переменной`'from_datacraft'`, как единое целое. Если указать `False`, можно извлечь какую-то часть, прописав её название в `etlcraft_path_for_config_{название конфига}`.

**Важно:** Перед запуском DAG необходимо заранее прописать все необходимые данные в разделе Admin > Variables. Для каждого конфига, указанного в `config_names`, нужно задать следующие переменные:

1. **Формат конфигурации:** `etlcraft_format_for_config_{название конфига}`. Эта переменная определяет формат данных, например, `json` или `yaml`.

2. **Путь к конфигурации:** `etlcraft_path_for_config_{название конфига}`. Здесь указывается путь, по которому нужно искать конфиги.

3. **Тип конфигурации:** `etlcraft_source_for_config_{название конфига}`. Эта переменная определяет источник данных. Существует три основных типа конфигураций:
   - `file`: Конфигурация загружается из файла.
   - `datacraft_variable`: Конфигурация загружается из переменной `from_datacraft`.
   - `other_variable`: Конфигурация загружается из другой переменной.

Убедитесь, что все переменные правильно заданы, чтобы задача могла корректно собрать все необходимые данные.
""",
        config_names=['from_datacraft', 'stages', 'presets', 'metadata'],
        namespace="etlcraft",
        entire_datacraft_variable=True
    )
    
    collect_configs_result = XComArg(collect_configs)
    
    clear_models_dir_task = clear_models_dir(models_dir)
    
    input_data_for_creat_models_func = prepare_input_data_for_create_models_func(models_dir, collect_configs_result)
    
    
    @task( map_index_template="{{ my_custom_map_index }}", #map_index_template="{{ my_custom_map_index }}" - это нужно для формироания индексов!! вот тут исходник https://www.astronomer.io/docs/learn/dynamic-tasks
          doc_md=f"""**Описание задачи:** 
Эта задача отвечает за создание файлов моделей dbt на основе переданных данных: имени файла, пути и содержимого.

**Входные данные:**
- **file_name:** Имя файла, который будет создан.
- **path:** Путь, по которому будет сохранён файл.
- **content:** Содержимое файла (модели от которых зависит и вызов макроса).

**Выходные данные:** Сообщение о создании файла, включающее его имя и путь.

**Процесс выполнения:**
1. Сначала функция создает все необходимые директории, если их нет, используя `os.makedirs(path, exist_ok=True)`.
2. Затем создается файл по указанному пути с переданным содержимым.
3. На выходе возвращается сообщение о том, что файл успешно создан.
""") 
    def create_models_file(file_name, path, content):
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = "Creat " + str(file_name)
        
        os.makedirs(path, exist_ok=True)
        # Ваш код для создания файла
        with open(f"{path}/{file_name}", 'w') as f:
            f.write(content)
        return f"File {file_name} created at {path}"
    
    
    # Функция для подготовки данных для каждого подуровня (вытаскивает данные для каждого подслоя, иначе не получается) 
    # (решили, что потом заменим оператор CollectConfigsOperator просто на функцию, тогда этот шаг возможно будет не нужен, 
    # отмечу, что сейчас ф-я prepare_input_data_for_create_models_func, которая готовит данные для create_models_file - это таск, а результаты таска, можно использовать только в другом таске
    # если ъотим это упростить, то тогда функцию prepare_input_data_for_create_models_func тоже надо будет сделать просто функцией, а не таском!!!))
    @task
    def prepare_files_data(stage, sublayer, input_data):
        logger = logging.getLogger("airflow.task")
        # Проверка наличия данных для заданного слоя и подуровня
        if stage not in input_data or sublayer not in input_data[stage]:
            logger.info(f"Не было полученно данных для создания файлов моделей на слое {stage}")
            return []
        return input_data[stage][sublayer]

    # Создание основной TaskGroup для всех моделей
    with TaskGroup(group_id="create_models_group") as create_models_group:
        for stage, sublayers in stages.items():
            with TaskGroup(group_id=stage) as stage_tg:
                for sublayer in sublayers:
                    with TaskGroup(group_id=sublayer) as sublayer_tg:
                        # Подготовка данных для текущего подуровня
                        files_data = prepare_files_data(stage, sublayer, input_data_for_creat_models_func)
                        
                        # Проверяем, есть ли данные для создания задач
                        #if files_data :
                            # Используем expand_kwargs с подготовленными данными
                        create_models_file.expand_kwargs(files_data)
                        #else:
                            #logger.info(f"Не было полученно данных для создания файла модели на слое {stage}")

    # Определение зависимостей между задачами
    collect_configs >> clear_models_dir_task >> input_data_for_creat_models_func >> create_models_group

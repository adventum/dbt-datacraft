---
description: Источники данных, на каждый из которых в Airbyte нужно настроить Source и Destination
config_default_type: "`datacraft_variable`"
config_default_format: "`json`"
type: config
doc_status: ready (нужно ревью)
---
# Описание

Данный конфиг формируется/обновляется при добавлении/изменении источников данных в **dataCraft** в разделе **Данные**:
![[add_attr_model_in_dataCraft_example.jpg]]

Конфиг содержит следующую информацию об источнике данных:
- `id` - идентификатор
- `source_type` - тип источника
- `account_name` - название учетной записи или профиля в Яндекс.Директ, Google Ads и других аналитических системах
- `project` - проект, к которому относятся данные источника
- `preset` - название пресета. Указанное в этом разделе название, должно соответствовать названиям пресетов в разделе `source_presets` в конфиге [[presets]]
- `source_class` - класс источника данных. Всего разделяем источники на 4 класса: `ads_cabinet`, `analytical_service`, `crm` и `other`

Данные из конфига используются для создания файлов моделей в DAG’е [[generate_models]]. Конфиг обновляется при каждом внесённом изменении в раздел **Источники данных** в **dataCraft**. Это позволяет поддерживать модели по обработке данных актуальными. 
# Пример

```jsx
{
    "datasources": {
        "13": {
            "source_type": "yd",
            "account_id": "adventum-client2",
            "project": "datacraft",
            "preset": "yd_default", 
            "source_class": "ads_cabinet",
            "credentials_craft_token_id": 1
        },
        "14": {
            "source_type": "ym",
            "account_id": "adventum-client2",
            "project": "datacraft",
            "preset": "ym_default", 
            "source_class": "ads_cabinet",
            "credentials_craft_token_id": 2
        },
		"15": {
            "source_type": "appmetrica",
            "account_id": "adventum-client2",
            "project": "datacraft",
            "preset": "appmetrica_default", 
            "source_class": "ads_cabinet",
            "credentials_craft_token_id": 3
        }
    }
}
```

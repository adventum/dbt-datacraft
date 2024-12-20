---
term_name: Прямая интеграция
description: Данные, которые должны попадать в систему не через Airbyte, а загрузкой непосредственно в базу данных
type: term
doc_status: ready (нужно ревью)
---

В **dataCraft Core** предусмотрена возможность загрузки данных напрямую в базу данных без использования [[Airbyte Connector|коннектора в Airbyte]]. Данные, которые загружаются напрямую, и называются **прямой интеграцией (direct integration)**. 

**Прямые интеграции актуальны в следующих случаях:**
- Когда данные поступают из внутренних систем компании, к которым удобнее обращаться напрямую, чем через Airbyte. Например, данные из корпоративного хранилища данных (КХД).
- Когда требуется интеграция с системами, для которых не существует готовых коннекторов Airbyte. Например, это могут быть самописные CRM.
- Когда необходима минимизация зависимости от промежуточного слоя и максимизация скорости загрузки данных в хранилище.

Данные о прямых интеграциях хранятся в конфиге [[presets]] в разделе `directintegration_presets`. 
```jsx
{   "directintegration_presets": {
    "example1": {
        "schema": "название схемы",
        "table": "название таблицы",
        "fields": ["field1", "field2", "field3"],
        "entities": ["entity1", "entity2"] 
    }       
        },
"source_presets": {
<...>
}
}
```

Каждый объект внутри `directintegration_presets` описывает одну прямую интеграцию и имеет следующие параметры:
- **schema**: указывает название схемы в базе данных.
- **table**: имя таблицы, в которую загружаются данные.
- **fields**: массив, описывающий конкретные поля (колонки) таблицы, которые будут использоваться.
- **entities**: массив, описывающий типы [[Entity|сущностей]] данных, которые загружаются. 

Данные из `directintegration_presets`  используются для создания [[Model|файлов моделей]] с помощью DAG’а [[generate_models]]. Также данные о прямых интеграциях отображаются в интерфейсе **dataCraft**. Это позволяет использовать их при формировании наборов данных. 
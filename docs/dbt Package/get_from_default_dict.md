---
category: auxiliary
step: 1_silos
sub_step: 1_normalize
in_main_macro: normalize
doc_status: ready
in_aux_macro: find_incremental_datetime_field
---
# macro  `get_from_default_dict`

## Описание

Этот макрос принимает путь (в виде списка ключей) и словарь в качестве входных данных, а затем проходит по словарю, следуя указанному пути. Если он успешно достигает конца пути, он возвращает значение в указанном месте в словаре. Если встречается ключ, которого нет в словаре до достижения конца пути, возвращается значение по умолчанию.

## Аргументы

- `default_dict`: Словарь для обхода.
- `path`: Список ключей, представляющих путь для обхода в словаре. Ключи должны быть предоставлены в том порядке, в котором к ним будет осуществляться доступ.
- `default_return`: (Необязательно) Значение, которое вернется, если конец пути не может быть достигнут. Если не предоставлено, возвращается пустая строка.

## Функциональность

```dbt
{% set result = get_from_default_dict(['ключ1', 'ключ2'], my_dict) %}
```

Это приведет к обходу `my_dict`, сначала переходя к ключу1, а затем к ключу2. Если ключ2 существует в словаре, расположенном по ключу1, возвращается значение. В противном случае возвращается пустая строка (или значение, указанное в `default_return`).

Пример

Учитывая следующий словарь:

```dbt
{% set my_dict = {
    'fruit': {
        'color': 'red',
        'name': 'apple',
        'details': {
            'origin': 'Washington',
            'variety': 'Red Delicious'
        }
    },
    'animal': {
        'type': 'dog',
        'name': 'Rex',
        'details': {
            'breed': 'Labrador',
            'color': 'Black'
        }
    }
} %}
```

В этом примере будет возвращено  `'red'`:

```dbt
{% set result = datacraft.get_from_default_dict(['fruit', 'color'], my_dict) %}
```

В этом примере будет возвращено  `'Labrador'`:

```dbt
{% set result = datacraft.get_from_default_dict(['animal', 'details', 'breed'], my_dict) %}
```

В этом примере будет возвращено  `'Washington'`:

```dbt
{% set result = datacraft.get_from_default_dict(['fruit', 'details', 'origin'], my_dict) %}
```

В этом примере будет возвращено  `'dog'`:

```dbt
{% set result = datacraft.get_from_default_dict(['animal', 'type'], my_dict) %}
```

В этом примере будет возвращена пустая строка (since there is no `'taste'` key under `'fruit'`):

```dbt
{% set result = datacraft.get_from_default_dict(['fruit', 'taste'], my_dict) %}
```
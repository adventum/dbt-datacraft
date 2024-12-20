---
category: sub_main
step: 6_attribution
sub_step: 6_create_missed_steps
in_main_macro: attr
doc_status: ready
---
# macro `attr_create_missed_steps`

## Список используемых вспомогательных макросов

Без вспомогательных макросов.
## Описание

Это шестой шаг макроса `attr`. В этом макросе создаются отсутствующие шаги в атрибуции. Он обрабатывает случаи, когда некоторые шаги воронки не были зарегистрированы.

## Аргументы

Этот макрос принимает следующие аргументы:
```sql
  params = none,
  funnel_name=none,
  limit0=none
```
## Функциональность

Сначала в макросе происходит настройка материализации данных: устанавливается порядок сортировки данных по идентификатору группы, дате, ссылке и идентификатору.

Далее идёт расчёт максимального приоритета для каждой группы (`qid`) и периода.

Затем - генерация всех приоритетов для каждой группы и каждого периода.

И после этого - создание всех возможных комбинаций пропущенных шагов для каждой группы и каждого приоритета.

Из полученного запроса делается выборка данных с добавлением порядкового номера строки (`__rn`) для каждой группы.

Если аргумент `limit0` активирован, то в конце SQL-запроса будет добавлено `LIMIT 0`.
## Пример

Файл в формате sql в папке models. Название файла `attr_myfirstfunnel_create_missed_steps`

Содержимое файла:
```sql
-- depends_on: {{ ref('attr_myfirstfunnel_calculate_period_number') }}

{{ datacraft.attr() }}
```

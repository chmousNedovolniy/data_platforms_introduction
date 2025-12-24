# Практическое задание 6

## Подключение к Greenplum, проверка подключения к базе данных и pdfdist

Приступим к выполнению задания. Подключимся к GreenPlum (использовался PuTTY с Windows). Введём наши данные и проверим, что psql установлен на сервере, а также зайдём внутрь базы idp и взглянем на неё:

![0-intro-idp](images/0-intro-idp.JPG)

Можно заметить и нашу команду team_14_managed (этот скрин сделан уже после выполнения задания).

Далее подключимся к pgfdist, используем данные с семинара (датасет Iris.csv лежит в папке data):

![1-pgfdist-check](images/1-pgfdist-check.JPG)

## Создание EXTERNAL таблицы, проверка работы

И создадим по нему EXTERNAL таблицу:

![2-create-external](images/2-create-external.JPG)

Проверим, что таблица создана, увидим, что данные в неё подкачиваются из pgfdist динамически через HTTP-соединение, так как они не хранятся физически в базе.

![3-SELECT-1](images/3-SELECT-1.JPG)

![4-SELECT-2](images/4-SELECT-2.JPG)

Аналогичное "полотно" будет выведено и про количество строк в таблице, оставим только результат, чтобы убедиться, что переданы все данные.

![5-rows-check](images/5-rows-check.JPG)

## Создание MANAGED таблицы, проверка работы

Далее создадим MANAGED таблицу.

![6-create-managed](images/6-create-managed.JPG)

Передадим в неё данные из EXTERNAL таблицы, снова наблюдаем подключение к pgfdist.

![7-data-transfer-1](images/7-data-transfer-1.JPG)
![8-data-transfer-2](images/8-data-transfer-2.JPG)

Однако при последующих запросах такого подключения уже нет, так как MANAGED таблица хранит данные уже в хранилище Greenplum.

![9-SELECT-managed](images/9-SELECT-managed.JPG)

В конце удалим использованную EXTERNAL таблицу.

![10-drop-external](images/10-drop-external.JPG)

Таблицу можно найти в базе данных по имени team_14_managed. Все использованные изображения находятся в хронологическом порядке в папке images.



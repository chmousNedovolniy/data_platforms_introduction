# Практическое задание 4 — Apache Spark on YARN + HDFS + Hive

Цель: запустить Spark под управлением YARN, подключиться к HDFS, прочитать данные, выполнить трансформации, записать с партиционированием как таблицу Hive и проверить чтение стандартным клиентом Hive.

## Состав
- `scripts/spark-config.env.example` — пример конфига.
- `scripts/spark-deploy.sh` — установка Spark и конфиги (YARN + Hive Metastore).
- `scripts/spark-job.py` — PySpark‑джоб: чтение, трансформации, запись таблицы.
- `scripts/spark-run.sh` — запуск `spark-submit --master yarn`.

## Предпосылки
1. HDFS и YARN работают (задания 1 и 2).
2. Hive Metastore поднят, данные из задания 3 доступны в HDFS:
   - путь по умолчанию: `/user/hive/warehouse/demo.db/events`.
3. На NameNode открыт Metastore (9083).

Проверка HDFS:
```bash
sudo -S bash -lc "su - hadoop -c '/opt/hadoop/current/bin/hdfs dfsadmin -fs hdfs://192.168.1.99:8020 -report | head -n 40'"
```

Проверка данных в HDFS:
```bash
sudo -S bash -lc "su - hadoop -c '/opt/hadoop/current/bin/hdfs dfs -fs hdfs://192.168.1.99:8020 -ls /user/hive/warehouse/demo.db/events'"
```

## Конфиг
В репозитории уже есть готовый `scripts/spark-config.env` с дефолтными адресами
для учебной среды. Если адреса в вашей среде другие — просто поправьте файл любым
удобным редактором.

Если `sudo` требует пароль:
```bash
export SUDO_PASS=<пароль>
```

## Развертывание Spark
```bash
bash scripts/spark-deploy.sh scripts/spark-config.env
```
Что делает скрипт:
- устанавливает Spark на `SPARK_CLIENT_HOST`;
- кладет `spark-env.sh`, `spark-defaults.conf` и `hive-site.xml`;
- настраивает работу с YARN и Hive Metastore.

## Запуск Spark‑джоба под YARN
```bash
bash scripts/spark-run.sh scripts/spark-config.env
```
Джоб:
- читает CSV из HDFS;
- добавляет признаки (`event_upper`, `event_len`, `is_click`, `dt` из пути);
- пишет результат в таблицу Hive с партиционированием по `dt`.

## Проверка результата через Hive
Если `beeline` нет на узле запуска, подключитесь к `nn` и используйте путь из Hive:
```bash
ssh team@192.168.1.99 '/opt/hive/apache-hive-4.0.0-alpha-2-bin/bin/beeline -u jdbc:hive2://192.168.1.99:10000'
```
```sql
USE demo;
SHOW PARTITIONS events_spark;
SELECT COUNT(*) AS total_rows FROM events_spark;
```

Ожидается:
- таблица `demo.events_spark` существует;
- партиции `dt=2025-01-01` и `dt=2025-01-02`.

## Очистка
Удаление таблицы и данных:
```bash
/opt/hive/current/bin/beeline -u jdbc:hive2://<HIVESERVER2_HOST>:10000 <<'SQL'
DROP TABLE IF EXISTS demo.events_spark;
SQL
```

## Типовые ошибки
- `No valid local directories in property: mapreduce.cluster.local.dir` — нужно создать `/tmp/hadoop/mapred/local` и задать `mapreduce.cluster.local.dir` (в задании 1).
- `Failed to connect to 10000` — проверьте `hive-service.sh status`.
- `Datanode denied communication with namenode because hostname cannot be resolved` — отключить проверку hostname на NameNode.

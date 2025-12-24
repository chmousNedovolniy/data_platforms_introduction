# Практическое задание 5 — Spark on YARN через Prefect

Цель: реализовать поток (Prefect), который запускает Spark‑джоб под YARN, читает данные из HDFS, выполняет трансформации и сохраняет результат как таблицу Hive.

## Состав
- `scripts/prefect-config.env` — готовый конфиг под учебную среду.
- `scripts/prefect-config.env.example` — пример конфига.
- `scripts/prefect-deploy.sh` — установка Prefect (venv + pip).
- `scripts/prefect-run.sh` — запуск Prefect‑потока.
- `scripts/prefect-flow.py` — Prefect‑поток, который:
  - проверяет доступ к HDFS,
  - запускает `spark-submit` под YARN,
  - пишет данные в таблицу Hive.
- `scripts/spark-job.py` — PySpark‑джоб (чтение, трансформации, запись таблицы).

## Предпосылки
1. HDFS и YARN работают (задания 1 и 2).
2. Hive Metastore поднят (задание 3).
3. Данные для чтения есть в HDFS:
   `/user/hive/warehouse/demo.db/events`.

## Быстрый запуск
```bash
cd ~/task5
export SUDO_PASS=nm60x_fuDs
bash scripts/prefect-deploy.sh
bash scripts/prefect-run.sh scripts/prefect-config.env
```

## Проверка результата через Hive
Запуск `beeline` на `nn`:
```bash
ssh team@192.168.1.99 '/opt/hive/apache-hive-4.0.0-alpha-2-bin/bin/beeline -u jdbc:hive2://192.168.1.99:10000'
```
```sql
USE demo;
SHOW PARTITIONS events_prefect;
SELECT COUNT(*) AS total_rows FROM events_prefect;
```

## Что делает Spark‑джоб
- читает CSV из HDFS;
- добавляет признаки: `event_upper`, `event_len`, `is_click`, `dt` из пути;
- сохраняет в таблицу `demo.events_prefect` (parquet, partition by `dt`).

## Типовые ошибки
- `No valid local directories in property: mapreduce.cluster.local.dir` — создать `/tmp/hadoop/mapred/local` и задать `mapreduce.cluster.local.dir` (задание 1).
- `Failed to connect to 10000` — проверьте HiveServer2.
- `Permission denied` на HDFS — запускать от пользователя `hadoop`.

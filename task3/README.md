# Практическое задание 3 — развертывание Apache Hive (не embedded) и загрузка данных в партиционированную таблицу

Артефакт: bash-скрипты для развертывания Hive 4.0.0-alpha-2 с метастором в режиме сетевого сервиса (Apache Derby Network Server), запуск HiveServer2 для одновременной работы нескольких клиентов и загрузка данных в партиционированную таблицу.

## Состав
- `scripts/hive-config.env.example` — пример конфига.
- `scripts/hive-deploy.sh` — установка Hive, конфиги, подготовка HDFS.
- `scripts/hive-service.sh` — запуск/остановка/статус: Derby NS, Metastore, HiveServer2.
- `scripts/hive-load.sh` — подготовка данных, загрузка в HDFS и наполнение партиций.

## Архитектура
- Метастор **не embedded**: отдельный Derby Network Server.
- `Hive Metastore` — отдельный сервис.
- `HiveServer2` — отдельный сервис для параллельных клиентов (`beeline`).

По умолчанию все сервисы размещаются на NameNode (192.168.1.99).

## Предпосылки
1. Hadoop/HDFS/YARN уже развернуты (задания 1 и 2).
2. HDFS в рабочем состоянии: DataNode подняты и видны в отчете.
   ```bash
   sudo -S bash -lc "su - hadoop -c '/opt/hadoop/current/bin/hdfs dfsadmin -fs hdfs://192.168.1.99:8020 -report | head -n 40'"
   ```
3. Открыты порты: 10000 (HiveServer2), 9083 (Metastore), 1527 (Derby).

## Конфиг
1. Скопируйте шаблон и заполните:
   ```bash
   cp scripts/hive-config.env.example scripts/hive-config.env
   nano scripts/hive-config.env
   ```
2. Обязательные поля:
   - `HIVESERVER2_HOST`, `METASTORE_HOST`, `DERBY_HOST`
   - `HDFS_NAMENODE_URI` (например, `hdfs://192.168.1.99:8020`)
   - `JAVA_HOME`, `HADOOP_HOME`
3. Если `sudo` требует пароль:
   ```bash
   export SUDO_PASS=<пароль>
   ```

## Развертывание
Из папки `task3`:
```bash
export SUDO_PASS=<sudo-пароль>   # если нужно
bash scripts/hive-deploy.sh scripts/hive-config.env
```

## Запуск/остановка сервисов
```bash
bash scripts/hive-service.sh scripts/hive-config.env start
bash scripts/hive-service.sh scripts/hive-config.env status
bash scripts/hive-service.sh scripts/hive-config.env stop
```

Что ожидается в `status`:
- процессы Derby/Metastore/HS2 через `jps`;
- открытые порты `1527`, `9083`, `10000`.

## Загрузка данных в партиционированную таблицу
```bash
bash scripts/hive-load.sh scripts/hive-config.env
```
Скрипт делает:
- БД `demo`
- Таблицу `events` (partitioned by `dt`)
- Загрузку CSV в HDFS и партиции `dt=2025-01-01`, `dt=2025-01-02`
- Проверку `SHOW PARTITIONS` и `COUNT(*)`

## Проверка вручную
```bash
/opt/hive/current/bin/beeline -u jdbc:hive2://<HIVESERVER2_HOST>:10000
```
В консоли Hive:
```sql
USE demo;
SHOW PARTITIONS events;
SELECT COUNT(*) AS total_rows FROM events;
```

Ожидаемый результат:
- две партиции: `dt=2025-01-01`, `dt=2025-01-02`
- итоговая строка `total_rows` (для тестовых данных = 5)

## Повторная загрузка (сброс данных)
Если запускали `hive-load.sh` несколько раз и нужно вернуть исходные 5 строк:
```bash
/opt/hive/current/bin/beeline -u jdbc:hive2://<HIVESERVER2_HOST>:10000 <<'SQL'
USE demo;
TRUNCATE TABLE events;
SQL

bash scripts/hive-load.sh scripts/hive-config.env
```

## Типовые ошибки
- `Datanode denied communication with namenode because hostname cannot be resolved`  
  Нужно отключить проверку hostname на NameNode (`dfs.namenode.datanode.registration.ip-hostname-check=false`) и перезапустить HDFS.
- `No valid local directories in property: mapreduce.cluster.local.dir`  
  Должен быть каталог `/tmp/hadoop/mapred/local` с правами `hadoop` (задача 1).
- `Failed to connect to 10000`  
  Проверьте `hive-service.sh status` и открытые порты.

## Очистка
```bash
bash scripts/hive-service.sh scripts/hive-config.env stop
for h in <all_hosts>; do ssh -i <key> <user>@$h "sudo rm -rf /opt/hive /var/log/hive /var/lib/hive-metastore"; done
```

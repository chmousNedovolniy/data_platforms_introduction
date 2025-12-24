# Практическое задание 1 — автоматизированное развертывание HDFS (NameNode, Secondary NameNode, 3 DataNode)

Артефакт: набор bash-скриптов для развёртывания и управления кластером HDFS на 4 ВМ с Ubuntu 24.04, Java 11. Скрипты не требуют Ansible и подходят для стенда с прямым SSH-доступом к нодам. Архив Hadoop скачивается один раз на управляющей машине и перекидывается на остальные хосты.

## Что входит
- `scripts/hdfs-config.env.example` — пример конфигурации (хосты, пути, версия Hadoop).
- `scripts/hdfs-deploy.sh` — идемпотентная установка Hadoop, пользователей, конфигов.
- `scripts/hdfs-service.sh` — форматирование NameNode, запуск/остановка/проверка демонов.

## Подготовка
1. Скопируйте пример конфига и отредактируйте под свои хосты:
   ```bash
   cp scripts/hdfs-config.env.example scripts/hdfs-config.env
   nano scripts/hdfs-config.env
   ```
2. Убедитесь, что с управляющей ВМ возможен SSH по ключу на все узлы (`SSH_USER`/`SSH_KEY`). Если `sudo` на узлах требует пароль, экспортируйте его как переменную `SUDO_PASS` перед запуском (пример: `export SUDO_PASS=nm60x_fuDs`), либо настройте NOPASSWD.
3. На управляющей ВМ нужно ~1 ГБ свободно в `/tmp` (кэш `hadoop-3.3.6.tar.gz`), портов/файрволлов на рабочих узлах не должно блокировать scp и SSH.
4. На всех ВМ должны быть открыты порты 9870 (NameNode HTTP), 8020 (NameNode RPC), 9868 (Secondary NN HTTP), 9864/9866/9867 (DataNode HTTP/transfer/IPC) и разрешён межузловой трафик.

## Установка кластера
Запускайте из каталога `task1` на управляющей ВМ:
```bash
export SUDO_PASS=<sudo-пароль>   # если sudo запрашивает пароль; иначе можно опустить
bash scripts/hdfs-deploy.sh scripts/hdfs-config.env
```
Скрипт:
- устанавливает пакеты (Java 11, rsync, tar), создаёт пользователя `hadoop`;
- скачивает Hadoop 3.3.6 на управляющую ВМ в `/tmp/hadoop-3.3.6.tar.gz` (повторно не качает) и копирует на каждый хост в `/tmp`;
- распаковывает на узлах в `/opt/hadoop` и создаёт симлинк `current`;
- создаёт каталоги данных/логов, выкладывает конфиги (`core-site.xml`, `hdfs-site.xml`, `hadoop-env.sh`, `workers`);
- настраивает `/etc/profile.d/hadoop.sh` для переменных среды.

## Первичное форматирование и запуск
После установки выполните:
```bash
bash scripts/hdfs-service.sh scripts/hdfs-config.env format
bash scripts/hdfs-service.sh scripts/hdfs-config.env start
```
Команды:
- `format` — однократно форматирует NameNode (повторно не запускайте на боевом кластере).
- `start`  — поднимает NameNode, Secondary NN и все DataNode.
- `status` — выводит `jps` по всем узлам.
- `stop`   — останавливает все демоны.

## Проверка целостности
- Веб-интерфейс NameNode: `http://<NAMENODE_HOST>:9870` — должно быть 3 живых DataNode.
- Логи: `/var/log/hadoop` на каждой ноде — не должно быть `ERROR`/`FATAL`.
- Командой `hdfs dfsadmin -report` (с NameNode под пользователем `hadoop`) проверить, что все 3 DataNode в статусе `In Service`.

## Замечания по безопасности
- Скрипт не настраивает Kerberos. Для продакшена требуется включить аутентификацию/авторизацию и ограничить SSH-ключи.
- Доступ к пользователю `hadoop` ограничьте нужными операторами.
- Проверяйте контрольные суммы дистрибутива Hadoop при необходимости (`HADOOP_TGZ_SHA256`).

## Очистка (если нужно пересобрать стенд)
```bash
bash scripts/hdfs-service.sh scripts/hdfs-config.env stop
for h in <all_hosts>; do ssh -i <key> <user>@$h "sudo rm -rf /opt/hadoop /hadoop /var/log/hadoop"; done
# при желании удалить локальный кэш архива
rm -f /tmp/hadoop-3.3.6.tar.gz
```

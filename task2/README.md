# Практическое задание 2 — развёртывание YARN (ResourceManager, 3 NodeManager, JobHistoryServer)

Артефакт: bash-скрипты для конфигурации YARN на уже установленном Hadoop (см. задание 1). Скрипты предполагают доступ по SSH к узлам и `sudo` (пароль передаётся через `SUDO_PASS`).

## Что входит
- `scripts/yarn-config.env.example` — пример конфига хостов/портов/директорий.
- `scripts/yarn-deploy.sh` — выкладывает конфиги YARN/MapReduce на все узлы.
- `scripts/yarn-service.sh` — запуск/остановка/статус ResourceManager, NodeManager и JobHistoryServer.

## Подготовка
1. Скопируйте пример конфига:
   ```bash
   cp scripts/yarn-config.env.example scripts/yarn-config.env
   nano scripts/yarn-config.env
   ```
   Проверьте хосты (RM, HistoryServer, список NodeManager), SSH-ключ, порты.
2. Убедитесь, что Hadoop уже установлен и сконфигурирован (задание 1), и тот же пользователь `hadoop` существует на всех нодах.
3. Если `sudo` требует пароль, экспортируйте его перед запуском: `export SUDO_PASS=<пароль>`.
4. Убедитесь, что порты открыты для внешнего доступа: 8088 (RM UI), 8042 (NM UI), 19888 (HistoryServer UI).

## Развёртывание конфигов YARN
Запускайте с управляющей ВМ из `task2`:
```bash
export SUDO_PASS=<sudo-пароль>   # если нужно
bash scripts/yarn-deploy.sh scripts/yarn-config.env
```
Скрипт:
- создаёт директории `/hadoop/yarn/local`, `/hadoop/yarn/logs`, `/var/log/hadoop-yarn`;
- заливает `yarn-site.xml`, `mapred-site.xml`, `yarn-env.sh`;
- прописывает `workers` аналогично NodeManager списку (для `start-yarn.sh` совместимости).

## Запуск сервисов
```bash
bash scripts/yarn-service.sh scripts/yarn-config.env start
```
Команды:
- `start`  — запускает ResourceManager, JobHistoryServer и все NodeManager.
- `stop`   — останавливает все демоны.
- `status` — показывает `jps` на всех узлах.

## Веб-интерфейсы (опубликовать для внешнего доступа)
- ResourceManager: `http://<RESOURCEMANAGER_HOST>:8088`
- NodeManager: `http://<NM_HOST>:8042` (каждый узел)
- JobHistoryServer: `http://<HISTORYSERVER_HOST>:19888`
Убедитесь, что firewall/NAT пропускает эти порты наружу.

## Проверка
- UI RM (8088) должен показывать 3 NodeManager в Live Nodes.
- `bash scripts/yarn-service.sh scripts/yarn-config.env status` — `ResourceManager`, `NodeManager`, `JobHistoryServer` в `jps`.
- Тестовая задача: `su - hadoop -c "yarn jar ${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar pi 2 10"` и проверка в RM UI/HistoryServer.

## Очистка
```bash
bash scripts/yarn-service.sh scripts/yarn-config.env stop
for h in <all_hosts>; do ssh -i <key> <user>@$h "sudo rm -rf /hadoop/yarn /var/log/hadoop-yarn"; done
```

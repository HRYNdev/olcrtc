# Локальная настройка Docker

Здесь описан один из способов запуска сервера olcrtc с локальной конфигурацией Docker.

## Идея

- держать изменяемые Docker-файлы в скрытой папке `.local`
- хранить конфигурационные файлы вне Git, в папке `.local`
- позволять пользователям обновлять репозиторий обычным `git pull`

## 1. Клонирование репозитория

```bash
git clone https://github.com/openlibrecommunity/olcrtc.git
cd olcrtc
```

## 2. Обновление до последней версии

Чтобы получить новую версию из upstream, выполните команду ниже:

```bash
git pull https://github.com/openlibrecommunity/olcrtc.git -recurse-submodules
```

## 3. Папка для локальных конфигураций

Создайте директорию `.local` в корне репозитория:

```bash
mkdir -p .local
```

Эта папка должна содержать файлы, которые будут использоваться только на вашей сервере.

## 4. Скопируйте docker-compose.yml в `.local`

Скопируйте файл ``docker-compose.yml`` (есть в репозитории), чтобы ваша локальная версия не перезаписывалась при следующем обноволении репозитория через ``git pull``:

```bash
cp docker-compose.server.yml .local/docker-compose.server.yml
```

Если файл `docker-compose.yml` позже изменится, скопируйте его снова этой же командой после `git pull`.

## 5. Создайте локальный файл окружения

Создайте `.local/.env` и заполните значения выполнения в соответствии с выбранным типом подключения.

Пример можно найти в `docs/examples/.env.telemost.server.example`.

## 6. Запуск OLCRTC

Запуск контейнеризированного сервера используя  ``docker-compose.server.yml`` и локальный ``.env``:

```bash
docker compose -f .local/docker-compose.server.yml --env-file .local/.env up -d
```

Проверка состояния контейнера:

```bash
docker compose -f .local/docker-compose.server.yml --env-file .local/.env ps
```

 Просмотр логов контейнера:

```bash
docker compose -f .local/docker-compose.server.yml --env-file .local/.env logs -f
docker logs olcrtc-server
```

## 7. Обновление контейнера

Запустить команду ниже для получения новой версии репозитория из облака:

```bash
git pull https://github.com/openlibrecommunity/olcrtc.git
```

После каждого обновления сравните новый и старый файл:

```bash
diff -wy .local/docker-compose.yml docker-compose.server.yml
```

Если есть отличия скопируйте файл из корня в папку ``.local``:

```bash
cp docker-compose.server.yml .local/docker-compose.server.yml
```

Затем перезапустите контейнер командами ниже:

```bash
docker compose -f .local/docker-compose.server.yml down
docker compose -f .local/docker-compose.server.yml --env-file .local/.env up -d
```

## Примечания

- Храните все локальные Docker-файлы внутри отдельной папки `.local`.
- Не добавляйте `.local` в репозиторий (она должна быть в файле ``.gitignore``)
- Держите общую документацию в `docs/`, а специфичные настройки в `.local`.

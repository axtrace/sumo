# Telegram Chat Summary Bot

[Sumo](https://t.me/sumo25_bot) - Бот для автоматической суммаризации чатов с сохранением истории в Yandex Database (YDB) и использованием YandexGPT для генерации кратких содержаний.

## 🌟 Особенности

- Сохраняет историю сообщений в YDB
- Генерирует краткие содержания чатов с помощью YandexGPT
- Ограничивает количество вызовов на пользователя
- Поддерживает команды с упоминанием бота (`@botname /summary`)
- Работает как в личных чатах, так и в группах

## ⚙️ Технологии

- **Yandex Cloud Functions** - хостинг бота
- **Yandex Database (YDB)** - хранение сообщений и контекста
- **YandexGPT** - генерация саммари
- **pyTelegramBotAPI** - работа с Telegram API
- **GitHub Actions** - CI/CD

## 🚀 Установка и развертывание

### Предварительные требования
- Аккаунт в [Yandex Cloud](https://yandex.cloud/ru)
- Telegram бот, созданный через [@BotFather](https://t.me/BotFather)
- Доступ к Yandex Database

### 1. Настройка окружения

Создайте файл `.env` в корне проекта:

```text
PRODUCTION_TOKEN=ваш_токен_бота
FOLDER_ID=идентификатор_каталога_YC
API_KEY=API-ключ_для_YandexGPT
YDB_ENDPOINT=grpcs://ydb.serverless.yandexcloud.net:2135
YDB_DATABASE=/ru-central1/.../... 
```

### 2. Развертывание в Yandex Cloud Functions

#### Вариант через командную строку

1. Установите [YC CLI](https://yandex.cloud/ru/docs/cli/)

2. Настройте профиль:
```bash
yc init
```
3. Разверните функцию:

```bash
yc serverless function create --name=sumo-bot
yc serverless function version create \
  --function-name=sumo-bot \
  --runtime python312 \
  --entrypoint main.handler \
  --memory 256m \
  --execution-timeout 10s \
  --source-path . \
  --environment "PRODUCTION_TOKEN=$PRODUCTION_TOKEN,FOLDER_ID=$FOLDER_ID,API_KEY=$API_KEY,YDB_ENDPOINT=$YDB_ENDPOINT,YDB_DATABASE=$YDB_DATABASE"
```

4. Настройка вебхука (опционально)
```bash
curl -F "url=https://functions.yandexcloud.net/ваша_функция" \
  "https://api.telegram.org/bot$PRODUCTION_TOKEN/setWebhook"
```

#### Вариант через UI-консоль
В [консоли](https://console.cloud.yandex.ru):
1. Создайте сервисный аккаунт и выдайте ему права editor в ydb и права invoker в functions
1. Создайте Cloud Function и укажите перемеенные окружения для неё из списка:
   ```text
      PRODUCTION_TOKEN=ваш_токен_бота
      FOLDER_ID=идентификатор_каталога_YC
      API_KEY=API-ключ_для_YandexGPT
      YDB_ENDPOINT=grpcs://ydb.serverless.yandexcloud.net:2135
      YDB_DATABASE=/ru-central1/.../... 
    ```
3. Привяжите к функции сервисный аккаунт, созданный ранее
   
   Опционально: можно настроить синхроинзацию через github actions

## 🛠 Команды бота

- `/start` - Приветственное сообщение

- `/summary` или `/summarize` - Сгенерировать краткое содержание чата. Также работает в формате @botname /summary

## 📊 Логика работы

1. Бот сохраняет все сообщения в YDB
2. По команде /summary:
- Проверяет лимиты вызовов по истории саммаризаций из YDB
- Получает сообщения из YDB с момента предыдущий саммаризации, если она была
- Формирует промпт для YandexGPT
- Получает саммаризацию от YandexGPT
- Отправляет пользователю структурированное содержание
- Сохраняет саммаризацию в историю

## 🤖 Пример работы
```makefile
Пользователь: /summary
Бот: 📝 Краткое содержание:

• Обсуждали интеграцию бота с YDB
• Решили использовать Python 3.12
• Определили структуру хранения сообщений
• Наметили план тестирования
```

## 📄 Лицензия
MIT License. См. файл LICENSE.

Разработано для Yandex Cloud. По вопросам и предложениям обращайтесь к автору.

# Notifications

Система асинхронных уведомлений из двух сервисов. Producer принимает уведомление по HTTP и отправляет его в Kafka. Consumer читает топик в составе consumer group, пишет уведомление в лог, обрабатывает его 500 мс и только после этого коммитит offset. Сообщения, которые не удалось разобрать, уходят в DLQ.

Go 1.25, Kafka 4.1 в режиме KRaft (без ZooKeeper). Клиент Kafka — `segmentio/kafka-go`, роутинг на стандартном `net/http`. В тестах testify.

## Запуск

Нужен Docker с compose.

```bash
make up
```

Команда создаст `.env` из `.env.example`, если его ещё нет, соберёт образы и поднимет Kafka, producer и три экземпляра consumer. Топики `user-notifications` (3 партиции) и `user-notifications.dlq` создаёт сервис `kafka-init` до старта приложений.

Отправить уведомление:

```bash
curl -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"user-1","type":"email","content":"Hello"}'
```

Логи и распределение партиций:

```bash
make logs
docker compose logs consumer | grep 'subscribed to'
```

Offset'ы и lag группы: `make groups`. Содержимое DLQ: `make dlq`.

Остановить вместе с данными: `make down`.

Тесты:

```bash
go test -race ./...
```

## Переменные окружения

Все переменные есть в `.env.example`.

| Переменная | По умолчанию | |
|---|---|---|
| `KAFKA_BROKERS` | `kafka:9092` | адреса брокеров через запятую |
| `KAFKA_TOPIC` | `user-notifications` | |
| `KAFKA_DLQ_TOPIC` | `user-notifications.dlq` | |
| `KAFKA_GROUP_ID` | `notification-processor` | consumer group |
| `KAFKA_WRITE_TIMEOUT` | `5s` | сколько producer ждёт запись в Kafka |
| `HTTP_PORT` | `8080` | порт producer на хосте |
| `SHUTDOWN_TIMEOUT` | `10s` | сколько ждать активные запросы при остановке |
| `STOP_GRACE_PERIOD` | `20s` | сколько docker ждёт остановки контейнера |
| `CONSUMER_REPLICAS` | `3` | количество экземпляров consumer |
| `LOG_LEVEL` | `info` | |

## API

`POST /v1/notifications`

```json
{"user_id": "user-1", "type": "email", "content": "Hello"}
```

| Код | |
|---|---|
| 202 | уведомление отправлено в Kafka |
| 400 | невалидный JSON или пустое поле |
| 413 | тело запроса больше 1 МБ |
| 503 | Kafka недоступна |

В Kafka ключ сообщения — `user_id`, тип передаётся в заголовке `type`, в теле остаются `user_id` и `content`.

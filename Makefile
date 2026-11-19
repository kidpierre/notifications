-include .env
HTTP_PORT ?= 8080

KAFKA := docker compose exec -T kafka /opt/kafka/bin
URL   := http://localhost:$(HTTP_PORT)/v1/notifications

.PHONY: env up logs topics partitions send send-order headers groups broken dlq test down

env:
	@test -f .env || { cp .env.example .env && echo ".env created"; }

up: env
	docker compose up --build -d

logs:
	docker compose logs -f producer consumer

topics:
	$(KAFKA)/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic user-notifications

partitions:
	docker compose logs consumer | grep 'subscribed to'

send:
	curl -s -X POST $(URL) -H 'Content-Type: application/json' \
		-d '{"user_id":"user-1","type":"email","content":"Hello"}'

send-order:
	for i in 1 2 3; do curl -s -X POST $(URL) -H 'Content-Type: application/json' \
		-d "{\"user_id\":\"user-7\",\"type\":\"sms\",\"content\":\"msg-$$i\"}"; done

headers:
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic user-notifications \
		--from-beginning --max-messages 5 --property print.key=true --property print.headers=true

groups:
	$(KAFKA)/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group notification-processor

broken:
	echo '{broken' | $(KAFKA)/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic user-notifications

dlq:
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic user-notifications.dlq \
		--from-beginning --timeout-ms 5000 --property print.headers=true

test:
	go test -race ./...

down:
	docker compose down -v

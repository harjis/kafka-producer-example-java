#!/bin/bash

docker-compose exec kafka \
kafka-console-consumer --bootstrap-server kafka:9092 --topic test-topic --from-beginning


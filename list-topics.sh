#!/bin/bash

docker-compose exec kafka \
kafka-topics --zookeeper zookeeper:2181 --list

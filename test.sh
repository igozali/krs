#!/bin/bash
# See https://stackoverflow.com/questions/59077877 regarding running
# a teardown function after `cargo run` finishes.
set -euo pipefail

docker-compose down || :
docker-compose up -d

while ! docker-compose exec kafka kafka-topics --zookeeper zookeeper:2181 --list; do
    echo "Waiting for Kafka broker to be ready"
done

cargo test $@
docker-compose down

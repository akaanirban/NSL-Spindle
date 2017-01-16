#!/bin/bash
source env.sh
echo "Hostname: $HOSTNAME"
export NUM_BROKERS=10
export COMPOSE_FILE='docker-compose.yml'
docker-compose up --no-recreate --build -d && \
    docker-compose scale kafka=$NUM_BROKERS

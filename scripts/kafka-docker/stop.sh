#!/bin/bash
source env.sh
docker-compose stop && \
    docker-compose rm -f

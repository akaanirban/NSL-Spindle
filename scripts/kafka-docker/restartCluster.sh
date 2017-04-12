#!/bin/bash
export HOSTNAME=`ifconfig | grep -oE "\binet ([0-9]{1,3}\.){3}[0-9]{1,3}\b" | grep -v "127.0.0.1" | grep -oE "\b([0-9]){1,3}.*\b"`
if [[ -z "$HOSTNAME" ]]; then
    echo "Failed to get inet IP"
fi
echo "Using hostname: $HOSTNAME"
docker-compose stop
docker-compose rm -f && docker-compose up -d

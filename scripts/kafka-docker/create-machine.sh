#!/bin/bash
source env.sh
docker-machine kill $DOCKER_MACHINE_NAME
docker-machine rm $DOCKER_MACHINE_NAME
docker-machine create -d virtualbox --virtualbox-memory 8192 $DOCKER_MACHINE_NAME
docker-machine regenerate-certs $DOCKER_MACHINE_NAME

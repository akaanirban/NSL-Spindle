#!/bin/bash
DOCKER_NAME=vehicle-dev
docker run --rm -d --name $DOCKER_NAME -v $HOME/.ivy2:/root/.ivy2 -v `pwd`:/root/dev/vehicle -v `cd ../Shared && pwd`:/root/dev/Shared -e DEBUG_VEHICLE=true -p9001:9001 wkronmiller/spindle-node:latest
docker exec -e DEBUG_VEHICLE=false -it $DOCKER_NAME sh -c 'echo "cd /root/dev/vehicle/Vehicle-Node" >> /root/.bashrc && echo "export TEST_QUERIES=globalSpeedAvg" >> /root/.bashrc; exec bash'
docker rm -f $DOCKER_NAME

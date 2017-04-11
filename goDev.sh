#!/bin/bash
DOCKER_NAME=vehicle-dev
docker run --rm -d --name $DOCKER_NAME -v $HOME/.ivy2:/root/.ivy2 -v `pwd`:/root/dev/vehicle -v `cd ../Shared && pwd`:/root/dev/Shared -P wkronmiller/spindle-node:latest
docker exec -it $DOCKER_NAME sh -c 'echo "cd /root/dev/vehicle/Vehicle-Node" >> /root/.bashrc; exec bash'
docker rm -f $DOCKER_NAME

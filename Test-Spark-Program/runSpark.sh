#!/bin/bash

MIDDLEWARE_ADDRESS=""
if [ $# -eq 1 ]; then
    echo "Using optional middleware address"
    MIDDLEWARE_ADDRESS="--env MIDDLEWARE_IP_OPTIONAL=$1"
fi

echo "going to use: $MIDDLEWARE_ADDRESS"

# runs spark in the docker container
if [[ "$(docker images -q nslrpi/test-spark:latest 2> /dev/null)" == "" ]]; then
  docker pull nslrpi/test-spark
fi
# assumes that the spindle bridge is all set up
docker run --rm -it --net=SPINDLE-BRIDGE ${MIDDLEWARE_ADDRESS} --name testSpark nslrpi/test-spark:latest
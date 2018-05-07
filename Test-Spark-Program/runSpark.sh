#!/bin/bash

# runs spark in the docker container
if [[ "$(docker images -q nslrpi/test-spark:latest 2> /dev/null)" == "" ]]; then
  docker pull nslrpi/test-spark
fi

# assumes that the spindle bridge is all set up
docker run --rm -it --net=SPINDLE-BRIDGE --name testSpark nslrpi/test-spark:latest
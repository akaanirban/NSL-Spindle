#!/bin/bash

# this script starts kafka and sets up the bridge network if it is not already set up
# RUN THIS FIRST!

# check that we have the container
# don't have to use the spotify kafka as the middleware, but this does provide a convenient setup
if [[ "$(docker images -q spotify/kafka:latest 2> /dev/null)" == "" ]]; then
  docker pull spotify/kafka
fi

CONTAINER_NAME=MIDDLEWARE-KAFKA
# clean up
docker kill $CONTAINER_NAME
docker network rm SPINDLE-BRIDGE

# create a bridge network
docker network create SPINDLE-BRIDGE

# parse out the correct ip
networkip=$(docker network inspect --format='{{json (index .IPAM.Config 0).Gateway}}' SPINDLE-BRIDGE)
networkip=$(sed -e 's/^"//' -e 's/"$//' <<<"$networkip")
firstThreeRe="(([0-9]{1,3})\.){3}"
parsedStart=$(echo $networkip | grep -o -E ${firstThreeRe})
fullIp=${parsedStart}2

# start the kafka
docker run -it --rm -d\
    --net=SPINDLE-BRIDGE\
    -p 2181:2181 -p 9092:9092\
    --env ADVERTISED_PORT=9092\
    --env ADVERTISED_HOST=$fullIp\
    --name=$CONTAINER_NAME\
    spotify/kafka
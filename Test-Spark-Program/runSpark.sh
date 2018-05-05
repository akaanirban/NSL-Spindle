#!/bin/bash

# runs the spark, automagically finds the ip address of the kafka container we need
MIDDLEWARE_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' MIDDLEWARE-KAFKA)
echo "middleware is:"
echo ${MIDDLEWARE_IP}
sbt run MIDDLEWARE_IP

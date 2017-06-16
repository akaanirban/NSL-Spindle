#!/bin/bash
#Stop all containers called SPINDLE
echo "Stopping all Spindle simulations"
docker kill $(docker ps|grep "SPINDLE"|awk '{print $1}')


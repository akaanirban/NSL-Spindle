#!/bin/bash

echo "Stoping all simulations"
echo "not aquiring logs!"
docker kill $(docker ps|grep "SPINDLE"|awk '{print $1}')
echo "Done"

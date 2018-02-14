#!/bin/bash
NODE_BASE_IMAGE_NAME=nslrpi/spindle-node
#NODE_BASE_IMAGE_NAME=anirbandas/spindle-node
#NODE_BASE_VERSION="0.0.1"
OUTPUT_TAG=$NODE_BASE_IMAGE_NAME:latest

function buildImage(){
	#VERSIONED_IMAGE="$NODE_BASE_IMAGE_NAME:$NODE_BASE_VERSION"
	# Build vehicle
    	# https://github.com/docker/docker/issues/6822#issuecomment-241281937
    	#docker build -t $VERSIONED_IMAGE -f VehicleDockerfile . && echo "Built base image with $JAR_NAME"
    	docker build -t $NODE_BASE_IMAGE_NAME -f VehicleDockerfile . && echo "Built base image with $JAR_NAME"
    	docker tag $NODE_BASE_IMAGE_NAME $NODE_BASE_IMAGE_NAME:latest
	#docker push $OUTPUT_TAG
}

buildImage

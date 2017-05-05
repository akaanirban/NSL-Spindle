#!/bin/bash

NODE_BASE_IMAGE_NAME=nslrpi/spindle-node-base
NODE_BASE_VERSION="0.0.1"

function buildBaseImage() {
    VERSIONED_IMAGE="$NODE_BASE_IMAGE_NAME:$NODE_BASE_VERSION"
    # Build vehicle
    # https://github.com/docker/docker/issues/6822#issuecomment-241281937
    docker build -t $VERSIONED_IMAGE -f VehicleDockerfile . && echo "Built base image with $JAR_NAME"
    #rm $JAR_NAME
    docker tag $VERSIONED_IMAGE "$NODE_BASE_IMAGE_NAME:latest" && echo "Built $NODE_BASE_IMAGE_NAME:latest"
}

BUNDLED_IMAGE_NAME="nslrpi/spindle-node:latest"
function buildBundledImage() {
    docker build -t $BUNDLED_IMAGE_NAME -f BundleDockerfile . && echo "Built $BUNDLED_IMAGE_NAME"
    docker push $BUNDLED_IMAGE_NAME
}


buildBaseImage && buildBundledImage

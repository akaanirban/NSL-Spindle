#!/bin/bash

NODE_BASE_IMAGE_NAME=nslrpi/spindle-node-base
NODE_BASE_VERSION="0.0.1"

function buildBaseImage() {
    VERSIONED_IMAGE="$NODE_BASE_IMAGE_NAME:$NODE_BASE_VERSION"
    # Build vehicle
    # https://github.com/docker/docker/issues/6822#issuecomment-241281937
    docker build -t $VERSIONED_IMAGE -f VehicleDockerfile . && echo "Built base image with $JAR_NAME"
    #rm $JAR_NAME
    docker tag $VERSIONED_IMAGE $NODE_BASE_IMAGE_NAME:latest
}

# For now keep base version number same as packaged version number
BUNDLED_IMAGE_NAME="$NODE_BASE_IMAGE_NAME-packaged:$NODE_BASE_VERSION"

OUTPUT_TAG=nslrpi/spindle-node:latest
function buildBundledImage() {
    docker build -t $BUNDLED_IMAGE_NAME -f BundleDockerfile .
    docker tag $BUNDLED_IMAGE_NAME $OUTPUT_TAG && echo "Built $OUTPUT_TAG"
    docker push $OUTPUT_TAG
}


buildBaseImage && buildBundledImage

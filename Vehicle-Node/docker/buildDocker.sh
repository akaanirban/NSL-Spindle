#!/bin/bash

NODE_BASE_IMAGE_NAME=wkronmiller/spindle-node-base
NODE_BASE_VERSION="0.0.1"

function buildBaseImage() {
    VERSIONED_IMAGE="$NODE_BASE_IMAGE_NAME:$NODE_BASE_VERSION"
    if [ -z "$JAR_NAME" ]; then
        echo "Missing JAR_NAME environment variable" && exit 1
    fi
    export JAR_NAME=$JAR_NAME
    # Build vehicle
    # https://github.com/docker/docker/issues/6822#issuecomment-241281937
    cat VehicleDockerfile | sed "s/{{jarName}}/$JAR_NAME/" | docker build -t $VERSIONED_IMAGE -
    rm $JAR_NAME
}

# For now keep base version number same as packaged version number
BUNDLED_IMAGE_NAME="$NODE_BASE_IMAGE_NAME-packaged:$NODE_BASE_VERSION"

function buildBundledImage() {
    docker build -t $BUNDLED_IMAGE_NAME -f BundleDockerfile .
}


buildBaseImage && buildBundledImage

#!/bin/bash

VEHICLE_ROOT_DIR=./

PROJECT_NAME=SpindleVehicle

DOCKER_DIR=docker/

JAR_DIR=$VEHICLE_ROOT_DIR/target/scala-2.11/

export JAR_NAME=$PROJECT_NAME-assembly-*.jar

function buildJar() {
    sbt assembly
}

function moveJar() {
    mv $JAR_DIR/$JAR_NAME $DOCKER_DIR
}

function buildDocker() {
   cd docker && ./buildDocker.sh && cd - 
}


echo "Packaging project" && buildJar && echo "Preparing docker image" && moveJar && buildDocker && echo "Done"

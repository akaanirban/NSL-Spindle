#!/bin/bash

VEHICLE_ROOT_DIR=./

PROJECT_NAME=SpindleVehicle

DOCKER_DIR=docker

JAR_DIR=$VEHICLE_ROOT_DIR/target/scala-2.11

JAR_REGEX=$PROJECT_NAME-assembly-*.jar

export JAR_NAME=vehicle.jar

function buildJar() {
    sbt assembly
}

echo "thru buildjar"

function moveJar() {
    JAR_SOURCE=`ls $JAR_DIR/$JAR_REGEX`
    #mv $JAR_SOURCE $DOCKER_DIR/$JAR_NAME
    cp -a src $DOCKER_DIR
    cp $JAR_SOURCE $DOCKER_DIR/$JAR_NAME
}

function buildDocker() {
   cd docker && ./buildDocker.sh && cd - 
}


echo "Packaging project\n\n\n" && buildJar && echo "Preparing docker image\n\n\n" && moveJar && echo "moved\n\n\n" && buildDocker && echo "Done"

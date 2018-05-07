#!/bin/bash


PROJECT_NAME=TestSpark
JAR_DIR=target/scala-2.11
DOCKER_DIR=docker
JAR_NAME=TestSpark.jar
JAR_REGEX=$PROJECT_NAME-assembly-*.jar

function buildJar() {
    sbt assembly
}

function moveJar() {
    JAR_SOURCE=`ls $JAR_DIR/$JAR_REGEX`
    #mv $JAR_SOURCE $DOCKER_DIR/$JAR_NAME
    cp -a src $DOCKER_DIR
    cp $JAR_SOURCE $DOCKER_DIR/$JAR_NAME
}

function buildDocker() {
    cd docker && ./buildDocker.sh && cd -
}

buildJar && moveJar && buildDocker
#!/bin/bash
source env.sh
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

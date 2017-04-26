#!/bin/bash
source env.sh
rm -rf /tmp/kafka*
rm -rf $KAFKA_HOME
curl http://mirror.symnds.com/software/Apache/kafka/0.10.1.0/kafka_2.11-0.10.1.0.tgz -o /tmp/kafka.tgz && \
  cd /tmp && \
  tar -xvf kafka.tgz && \
  rm -f ./kafka*.tgz && \
  mv kafka*/ $KAFKA_HOME && \
  echo "Installed kafka to $KAFKA_HOME" && \
  cd - && \
  cp config/server.properties $KAFKA_HOME/config/server.properties && \
  echo "KAFKA CONFIG: " && echo `cat $KAFKA_HOME/config/server.properties` && \
  exit 0

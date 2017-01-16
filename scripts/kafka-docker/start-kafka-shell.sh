#!/bin/bash
docker run --rm --link nslkafkacluster_zookeeper_1:zookeeper \
    --net nslkafkacluster_default \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e HOST=$HOSTNAME -e ZK=zookeeper -i -t wurstmeister/kafka /bin/bash

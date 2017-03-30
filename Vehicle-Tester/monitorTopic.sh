#1/bin/bash
docker exec -it nslkafkacluster_kafka_1 /opt/kafka/bin/kafka-console-consumer.sh --zookeeper 192.168.99.100:2181 --from-beginning --topic $1

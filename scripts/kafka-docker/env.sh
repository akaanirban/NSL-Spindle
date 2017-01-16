export COMPOSE_PROJECT_NAME='NSL-Kafka-Cluster'
export DOCKER_MACHINE_NAME=nsl
eval $(docker-machine env $DOCKER_MACHINE_NAME)
export HOSTNAME=`docker-machine ip $DOCKER_MACHINE_NAME`

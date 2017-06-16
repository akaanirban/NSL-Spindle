#!/bin/bash
# TO DO
if [ $# -eq 0 ]; then
    echo "No arguments provided"
    echo "Run it as : ./runSimulations.sh \$numberOfNodes || where \$numberOfNodes is the number of nodes you want"
    exit 1
fi

#close if there are any stray containers
docker kill $(docker ps|grep "SPINDLE"|awk '{print $1}')

# Write code to start up Cluster
echo "Starting the cluster head" 
#modify to change the Middleware host name everytime , if the host name has changed.
docker run -it --rm -d \
		-e MIDDLEWARE_HOSTNAME=192.168.0.9\
		-p 9001:9001 -p 2182:2182 -p 9093:9093\
		--name SPINDLE-CLUSTERHEAD\
		nslrpi/spindle-node
ClusterHeadIP=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' SPINDLE-CLUSTERHEAD`


#Set the middleware hostname, cluster head broker and zookeeper config for nodes
MIDDLEWARE_HOSTNAME=192.168.0.9
CLUSTERHEAD_BROKER=$ClusterHeadIP:9093	
CLUSTERHEAD_ZK_STRING=$ClusterHeadIP:2182


#for starting up the nodes
numberOfNodes=$1
nodeName=SPINDLE-NODE
for (( c=1; i<$numberOfNodes; i++ ))
do
	#docker run --rm -d --name $DOCKER_NAME --env --env-file ./env-list -p 9001:9001 -p 2182:2182 -p 9093:9093 nslrpi/spindle-node:latest
	echo "Starting Node $i"
	docker run -it --rm -d \
				-e MIDDLEWARE_HOSTNAME=192.168.0.9\
				-e CLUSTERHEAD_BROKER=$ClusterHeadIP:9093\
				-e CLUSTERHEAD_ZK_STRING=$ClusterHeadIP:2182\
				--name $nodeName$i\
				nslrpi/spindle-node
done
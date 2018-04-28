#!/bin/bash
# TO DO
if [ $# -eq 0 ]; then
    echo "No arguments provided"
    echo "Run it as : ./runSimulations.sh \$numberOfNodes || where \$numberOfNodes is the number of nodes you want"
    exit 1
fi

NUM_CLUSTERS=1

if [ $# -eq 2 ]; then
    echo "using $2 clusters"
    NUM_CLUSTERS=$2
fi

echo "num clusters:"
echo $NUM_CLUSTERS
#close if there are any stray containers
if [[ `docker ps|grep "SPINDLE"|awk '{print $1}'` ]]
then
   echo "Killing old Spindle processes"
   docker kill $(docker ps|grep "SPINDLE"|awk '{print $1}')
fi



# modify to change the Middleware host name everytime , if the host name has changed.
#####################################################
# NOTE: modify this variable when running the scrip
# you can find this address by running ifconfig
#####################################################
MIDDLEWARE_IP=192.168.133.161
numberOfNodes=$1
for (( clusters=0; clusters<$NUM_CLUSTERS; clusters++ ))
do
    # Write code to start up Cluster
    echo
    echo "Starting the cluster head"
    echo
    #ports are published to bind with the host for the cluster head
    docker run -it --rm -d \
            -e MIDDLEWARE_HOSTNAME=$MIDDLEWARE_IP\
            -e NODE_ID=0\
            -e NUM_NODES=$numberOfNodes\
            --name SPINDLE-CLUSTERHEAD$clusters\
            nslrpi/spindle-node
    ClusterHeadIP=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' SPINDLE-CLUSTERHEAD$clusters`


    #Set the middleware hostname, cluster head broker and zookeeper config for nodes
    MIDDLEWARE_HOSTNAME=$MIDDLEWARE_IP
    CLUSTERHEAD_BROKER=$ClusterHeadIP:9093
    CLUSTERHEAD_ZK_STRING=$ClusterHeadIP:2182


    #for starting up the nodes
    #also, ports are not published because the docker containers can talk with eachother if the ports are exposed only.

    ####################################################
    # NOTE: when I had it working the last was localhost and the others were from ifconfig
    nodeName=SPINDLE-CLUSTER$clusters-NODE
    for (( i=1; i<=$numberOfNodes; i++ ))
    do
        #docker run --rm -d --name $DOCKER_NAME --env --env-file ./env-list -p 9001:9001 -p 2182:2182 -p 9093:9093 nslrpi/spindle-node:latest
        echo
        echo "Starting Node $i"
        echo
        docker run -it --rm -d \
                    -e MIDDLEWARE_HOSTNAME=$MIDDLEWARE_IP\
                    -e CLUSTERHEAD_BROKER=$ClusterHeadIP:9093\
            -e NUM_NODES=$numberOfNodes\
                    -e CLUSTERHEAD_ZK_STRING=$ClusterHeadIP:2182\
            -e NODE_ID=$i\
                    --name $nodeName$i\
                    nslrpi/spindle-node
    done
done
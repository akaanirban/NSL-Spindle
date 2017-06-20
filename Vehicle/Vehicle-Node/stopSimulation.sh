#!/bin/bash
#Stop all containers called SPINDLE
logDirectory=Node-Logs #name of the logs directory
echo "Aquiring all the log files from nodes"

if [ -d "$logDirectory" ]; then
  # Control will enter here if $logDirectory exists.
  echo "Previous logs exist in $logDirectory folder"
  echo "Moving old logs to $logDirectory.bkp folder"
  mv $logDirectory $logDirectory.bkp
fi

echo "Copying all logs from Spindle Node containers to $logDirectory folder"
mkdir $logDirectory
cd $logDirectory

for x in `docker ps |grep SPINDLE-|awk '{print $NF}'`; 
  do  
    mkdir $x; 
    docker cp $x:/tmp/ $x/; 
    cd $x;
    mv tmp/*.csv .; 
    rm -rf tmp; 
    cd ..;
    echo "Aquired logs for $x" 
done;

echo "Completed aquiring logs"
echo "Stopping all Spindle simulations"
docker kill $(docker ps|grep "SPINDLE"|awk '{print $1}')
echo "Done"


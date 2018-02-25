#!/bin/bash
#Stop all containers called SPINDLE
logDirectory=LogOutput #name of the logs directory
echo "Aquiring all the log files from nodes"

if [ -d "$logDirectory" ]; then
  # Control will enter here if $logDirectory exists.
  echo "Previous logs exist in $logDirectory folder"
  echo "Deleting old logs"
  rm -rf $logDirectory
fi

echo "Copying all logs from Spindle Node containers to $logDirectory folder"
mkdir $logDirectory
cd $logDirectory

for x in `docker ps |grep SPINDLE-|awk '{print $NF}'`; 
  do  
    cat ""
    mkdir $x; 
    docker cp $x:/opt/output.log $x/; 
    cd $x;
    echo "for $x got"
    cat output.log | grep 'FINAL';
    echo "checking for errors:"
    cat output.log | grep 'BAD';
    cd ..;
done;

echo "combining"
find . | grep log | xargs cat | grep 'edu.rpi.cs.nsl.spindle.vehicle.gossip' > combined.final
echo "Completed aquiring logs"

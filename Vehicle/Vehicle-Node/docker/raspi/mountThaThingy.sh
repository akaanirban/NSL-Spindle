#!/bin/bash
mkdir -p mount
LOOPDEV=`losetup -f --show -P 2017-04-10-raspbian-jessie-lite.img`
P2=p2
FULLDEV=$LOOPDEV$P2
echo $FULLDEV
sleep 1
mount $FULLDEV ./mount

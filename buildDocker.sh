#!/bin/bash
cd Vehicle && git pull && cd -
docker build -t wkronmiller/nsl-spindle-simulator:latest .
docker push wkronmiller/nsl-spindle-simulator:latest 

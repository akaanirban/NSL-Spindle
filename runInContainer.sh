#!/bin/bash
# NOTE: this is used to work around the MacOS hard limit on the number of threads
# running at a single time. It is also probably not a terrible idea to target
# Linux instead of MacOS while developing the Vehicle simulator.
docker run --rm --name spindle-env -v `pwd`:/opt/ -it wkronmiller/scala-sbt

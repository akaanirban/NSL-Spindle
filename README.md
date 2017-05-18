# About

This repo contains the source code for the Spindle V2V distributed map/reduce system.
Spindle is written primarily in Scala and contains a number of sbt projects.

## Important Directories

`Shared` contains source code that is shared among multiple Spindle sbt projects.

`Spark`, contains the library source code for client Spark programs.

`Test-Spark-Program` contains an Apache Spark program that uses the Spindle Spark library to perform distributed map/reduce using Spindle.

`Vehicle` contains the source code for the Spindle software running on vehicle nodes as well as the source code for the vehicle network simulator.

The `docs` directory contains the source code for the <http://spindl.network> website.

# Setting up Dev/Test Environments

Currently, this system has only been tested on macOS Sierra, [CentOS 7](https://wiki.centos.org/Download), and [Raspbian](https://www.raspberrypi.org/downloads/raspbian/) 8 (Jessie).
As of this writing, all components of the system can run on macOS Sierra.
The Spindle vehicle node software runs on Raspbian and is compatible with Raspberry Pi 2 Model B and should be compatible with the Model 3 as well.
The "cloud" Kafka cluster is known to run on CentOS and should also work fine on Ubuntu and most other mainstream linux distributions.

To develop and build software in this repository, you will need to install [SBT](http://www.scala-sbt.org/0.13/docs/Setup.html).
I (Rory) also strongly suggest using IntelliJ instead of Eclipse to do development.
You can find information about [importing SBT to IntelliJ](https://www.jetbrains.com/help/idea/2017.1/getting-started-with-sbt.html#import_project).

To run the Spark programs, download [Spark 2.0.1](http://spark.apache.org/releases/spark-release-2-0-1.html) and add the `bin` and `sbin` folders to your [PATH environment variable](https://superuser.com/questions/284342/what-are-path-and-other-environment-variables-and-how-can-i-set-or-use-them).

The "cloud" server should be using [Apache Kafka 0.10.2.0](https://www.apache.org/dyn/closer.cgi?path=/kafka/0.10.2.0/kafka-0.10.2.0-src.tgz).
You will want to take a look at the [documentation](https://kafka.apache.org/0102/documentation.html).
For information about starting up the cloud kafka cluster, see the [quick start information](https://kafka.apache.org/0102/documentation.html#quickstart).

## Project Configuration Files

Spindle uses the [Typesafe Config library](https://github.com/typesafehub/config).
The configuration files are located in `src/main/resources/application.conf`.
The configurations are loaded by objects declared in `Configuration.scala`.

To understand how a particular program can be configured, take a look at its `application.conf`.

Of particular note is when a configuration parameter is delcared twice where the second declaration looks something like: `foo.bar.baz=${?BIZZ_BUZZ}`.
This syntax means you can configure the `foo.bar.baz` property by setting the environment variable `BIZZ_BUZZ` before starting the program.
If no environment variable is set, the default value (specified in the first of the two declarations) is used.
In this case, it is a good idea to use environment variable rather than changing the default value.

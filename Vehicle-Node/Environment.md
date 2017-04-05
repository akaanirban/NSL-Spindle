Notes from Skype w/Mike Wittie, Patterson meeting April 5, 2017

# About

- DSRC supports IP stack
- Uplink to cloud done over cellular modems
- Dynamically assigned ipV6 address
    - Verion LTE
- Hardware
    - Raspbery pi 3

## How to handle clusterhead selection

1. Designate single clusterhead

- Hard-coded IP addresses, hard-coded IP address for clusterhead
    - Create mock addressing service
    - Mock sensor data based on pre-generated tests
- Defer dynamic clustering 

## Placement of Kafka Cluster

- Different region based on where testing is done
    - US-East 1 for NY

## Sensors

- Have single custom-built sensor
    - Measuring
        - accelerometer
        - temp, humidity
            - road
            - tire
        - laser rangefinder: measuring road surface
            - gorund
            -tire
        - GPS
- Thinking of preloading each raspi with traces

### How data is ingested

- Currently in a MySQL database
- Want to publish data directly to kafka

- Sensors connected to arduino board
    - then sent to raspberry pi
- We assume sensor readings are published to kafka

#TODOs

- Come up with meaningful map/reduce queries
    - Figure out what data we need for them
    - Assume live vehicle cluster is part of larger simulated set of clusters
    - Mike seems to be focusing on weather-based road conditions
    - Current dataset is mostly accelerometer-based
- Get sql schema from Mike and adapt to Kafka message format

## Still Need to Figure out

- Setting up IP addresses

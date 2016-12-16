# Spindle-Vehicle

Spindle softare running on vehicles.

## Desigin Decisions

- Send messages ever `delta/k` times
    - `delta` is Spark Streaming window time
- Vehicles send data continuously
    - User must expect to get more than one message per vehicle per batch

## Testing Environment

- Each vehicle is told what cluster it is in
- Data available to vehicle simulator node
    - Location
    - Speed
    - Beacons from other vehicles received in some interval
        - Indicates vehicles that are reachable in that interval
    - Probability of a message successfully being sent to another vehicle
        - Connectivity

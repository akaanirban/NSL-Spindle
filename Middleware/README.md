# About

This directory contains the source code for the NSL Spindle architecture's cloud middleware software.

## What it Does

The middleware is responsible for the following functions:

- [ ] Managing a list of currently connected Spark clients and their queries
- [ ] Managing a list of currently active derived queries
- [ ] Routing incoming vehicles to Spark clients


# Questions/Architectural Decisions

## Middlware Relay

There are a number of options for implementing the middleware relay:

1. Pull the existing logic out of the Vehicle repo and make the Middleware an akka monolith
    - Pros: very little new codidng, sharing codebase with Vehicle
    - Cons: unreliable shut-down of mappers, poor scaling
2. Run each mapper as a separate executable
    - Pros: 
        - large parts of the executable can still be taken from/shared with vehicle code base
        - can containerize/distribute mappers for a high degree of horizontal scalability
        - potentially less buggy/more responsive than option 1
    - Cons: 
        - slightly more difficult to test
        - robust implementation requires monitoring 
        - middleware architecture will diverge from Vehicle architecture (raises maintainability questions)
3. Write mapper from scratch as an Akka Streams actor using the Kafka plugin for Akka Streams
    - Pros:
        - Maintain monolithic codebase for mapper (easier to debug, version)
        - Have some horizontal scalability: scale actors out
    - Cons:
        - Maintaining state (maintaining offsets) becomes more difficult
        - Scaling trickier than soltuion 2
        - Middlware will have similar functionality to Vehicle mapper but completely different architecture

WRK suggestion: I think solution 2 will be significantly easier to debug than solution 1 and faster to implement than solution 3. I believe that implementing solution 2 will help avoid scalability/responsiveness problems and think that if we get good results from solution 2, we can replace the current Vehicle mappers with the mappers from solution 2.

## Other Questions/Concerns

- How do we propagate query lists to vehicles?
- We should implement receivers for vehicle data
    - I believe we should not allow vehicles to publish directly to Kafka topics b/c security concerns of exposing Kafka to open web
        - Could use new Kafka encryption features, but kafka's new features seem to be buggy. Also, from an architectural standpoint, having an endpoint we control gives us more flexibility.
- Should this system be written in Scala or should as much be written in Java as possible?

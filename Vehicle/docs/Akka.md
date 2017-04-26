# Akka

Information about the Akka framework as it relates to use in a V2V setting.

## Related Projects

### Lightbend Case Studies

- [Eero Home WiFi](https://www.lightbend.com/resources/case-studies-and-stories/how-eero-disrupts-consumer-wifi-with-highly-reliable-systems-powered-by-akka-and-reactive)
- [Samsung IoT](https://www.lightbend.com/resources/case-studies-and-stories/samsung-strategy-and-innovation-center-executes-iot-vision-at-startup-speed-with-reactive-architecture)
    - Akka on cloud to receive and process IoT data
    - Akka Cassandra integration
- [Xebia](https://www.lightbend.com/resources/case-studies-and-stories/lightbend-aids-in-train-safety-in-the-netherlands)
    - Keeps track of dangerous compounds being shipped on Dutch trains
    - Cloud HTTP endpoint runs Akka
- [MAR Border Monitoring](https://www.lightbend.com/resources/case-studies-and-stories/keeping-borders-safe-with-akka)
    - "The first thing that drew Raymond to Akka was how easy it was to work with Akka Actors, subsequently simplifying the problem of processing the raw sensor data and aggregating it to one vehicle view asynchronously"
    - "the cellular technology utilized for data transmission required that a balance be struck between available speed and bandwidth, which was easy to do in the Actor model as it’s designed with lightweight protocols in mind"
    - "Akka is running on both industrial servers (a fit for purpose server that can operate in a higher range of temperature, vibrations and other environmental aspects) in the patrol cars, on sensor arrays, and on a central clustered system in a data center that coordinates all the sensors and data.

    Not only does the system aggregate the raw data of the sensors, but it also does the processing as well (license plate, country recognition, vehicle classification) which is an extremely CPU intensive task that is executed asynchronously via Akka actors."

## Technology Break-Down

- [Remoting](http://doc.akka.io/docs/akka/2.4.14/scala/remoting.html)
    - Allows communication over a network
    - Transport layer is "pluggable" but defaults to TCP
    - "Peer-to-peer" and expects "symmetric" paths between nodes (i.e. no NAT or interference from firewalls)
- [Actor References](http://doc.akka.io/docs/akka/2.4/general/addressing.html)
    - "Since actors are created in a strictly hierarchical fashion, there exists a unique sequence of actor names given by recursively following the supervision links between child and parent down towards the root of the actor system."
    - Remote TCP format: `"akka.tcp://[actor_system_name]@[hostname]:[port]/[grandparent]/[parent]/[actor]"`
- [Akka IO Layer](http://doc.akka.io/docs/akka/2.4/dev/io-layer.html#io-layer)

### [Messaging Gaurantees](http://doc.akka.io/docs/akka/2.4/general/message-delivery-reliability.html)

- "At-most-once"
- "message ordering per sender-receiver pair"
    
### [Clustering](http://doc.akka.io/docs/akka/2.4.14/common/cluster.html#cluster)


#### Gossip Protocol

"The cluster membership used in Akka is based on Amazon's Dynamo system and particularly the approach taken in Basho's' Riak distributed database. Cluster membership is communicated using a Gossip Protocol, where the current state of the cluster is gossiped randomly through the cluster, with preference to members that have not seen the latest version."

- [Riak Gossip](http://docs.basho.com/riak/kv/2.2.0/learn/glossary/#gossiping)
- [Riak Core Gossip Source](https://github.com/basho/riak_core/blob/master/src/riak_core_gossip.erl#L105)

#### [Akka Cluster Source Code](https://github.com/akka/akka/blob/master/akka-cluster/src/main/scala/akka/cluster/Cluster.scala)

- Each "cluster member" requires a UID
- Depends on [Scheduler](http://doc.akka.io/docs/akka/current/java/scheduler.html)
    - Sub-second resolution
- [Joining](http://doc.akka.io/docs/akka/current/java/scheduler.html)
    - Nodes join cluster when members send `Join(selfAddress)`
    - "An actor system cna only join a cluster once. Additional attempts will
    be ignored. When it has successfully joined it must be restarted to be able
    to join another cluster or to join the same cluster again."
- [Example Configuration](http://doc.akka.io/docs/akka/rp-15v09p03/general/configuration.html)
    - Gossip every second
    - TTL 2 seconds

### [Routing](http://doc.akka.io/docs/akka/2.4.14/scala/routing.html#routing-scala)

"Messages can be sent via a router to efficiently route them to destination actors, known as its routees. A Router can be used inside or outside of an actor, and you can manage the routees yourselves or use a self contained router actor with configuration capabilities."

"The settings for a router actor can be defined in configuration or programmatically."

"Sometimes, rather than having the router actor create its routees, it is desirable to create routees separately and provide them to the router for its use. You can do this by passing an paths of the routees to the router's configuration."

### [Streams](http://doc.akka.io/docs/akka/2.4/scala/stream/stream-flows-and-basics.html)

- Storm-style stream processing
- Rich operators, support for backpressure, computation graphs
- [Stream Cookbook](http://doc.akka.io/docs/akka/2.4/scala/stream/stream-cookbook.html#stream-cookbook-scala)
- [Integration](http://doc.akka.io/docs/akka/2.4.14/scala/stream/stream-integrations.html)
    - Can convert stream to set of actor messages
        - `Sink.afterRefWithAck`
        - `Sink.actorRef`
    - "[`Source.queue`](http://doc.akka.io/docs/akka/2.4.14/scala/stream/stream-integrations.html#Source_queue)
    can be used for emitting elements to a stream from an actor (or from anything running outside the stream). 
    The elements will be buffered until the stream can process them. You can offer elements to the queue and 
    they will be emitted to the stream if there is demand from downstream, otherwise they will be buffered 
    until request for demand is received."

#### Possible Implementation

- Router actor on each vehicle decides where to stream map results (i.e. which remote actor is cluster head receiver)
- Mapper actors send results to router
- Cluster Head receiver accepts/reduces/batches incoming data and sends to cloud

# Edgent

[Apache Edgent](https://edgent.apache.org/docs/home.html) is an edge computing platform written in Java (targeting Java 8). 

It is designed to run on low-power devices such as Raspberry Pi and Android systems where it can process and react to time-series data at the "edge."

Edgent, generally speaking, looks a lot like a stripped-down version of Spark Streaming, with the exception of being pure streaming rather than micro-batch.

## IBM Streams

Edgent seems to be intented to tie into a larger IBM product called [Streams](https://developer.ibm.com/streamsdev/new-to-streams/#Streams%20Quick%20Start%20Edition), which at first glance looks a bit like a competitor to [Talend](https://www.talend.com/products/real-time-big-data).

# Edgent Concepts

## Edgent Streams

"The fundamental building block of an Edgent applicationis a stream." 

Streams are defined as `TStream<T>` objects, which "are sourced, transformed, analyzed or sinked though functions, typically represented as lambda expressions, such as `reading -> reading < 50 || reading > 80` to filter temperature readings in Fahrenheit.

### Stream Operations

- Filter
- Split
- Union
- Partitioned Window
- Continuous Aggregation
- Batch
- Window: sliding window

- Map
- Sink

### Data Types

- `TStream<T>` represents a continuous stream of data
- `TWindow<T>` represents a batch of data from a `TStream<T>`
- `Provider` acts as a sort of container for topologies and applications
  - [IoTProvider](http://edgent.incubator.apache.org/javadoc/latest/index.html) can be used to add and remove topologies dynamically, (see [Loosely Coupled Edgent Applications](https://edgent.apache.org/recipes/recipe_dynamic_analytic_control.html#loosely-coupled-edgent-applications))
- `Topology` lays out a configuration for gathering and manipulating data
  - Support multiple "Analytic Flows" which can be activated and deactivated at runtime

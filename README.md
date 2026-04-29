# Java and Flink Streaming Short Course

This repository contains four small examples for a short bachelor-level class on Java and Apache Flink streaming.

All examples use the same story: temperature readings from rooms, encoded first as CSV-like strings:

```text
room,timestampMillis,temperatureCelsius
```

The shared streaming query is:

```text
current-time readings -> keep hot readings -> average by room in 20-second windows every 5 seconds
```

The examples are intentionally small and build on each other:

1. `01-plain-java-threads`: a hand-made streaming pipeline with Java threads and queues.
2. `02-flink-strings`: the same idea expressed as a Flink pipeline, still using raw strings.
3. `03-flink-tuples-local`: the Flink pipeline with `Tuple3<String, Long, Double>` and local execution.
4. `04-flink-cluster-lifecycle`: the tuple pipeline prepared for cluster submission, with a lifecycle and serialization demo.

## Prerequisites

- Java 11 or newer.
- Maven 3.8.6 or newer.
- Apache Flink 2.2.0 if you want to submit the fourth example to a real cluster.

## Quick Commands

Compile everything:

```bash
mvn clean package
```

Run a single example locally:

```bash
mvn -pl 01-plain-java-threads compile exec:exec
mvn -pl 02-flink-strings compile exec:exec
mvn -pl 03-flink-tuples-local compile exec:exec
mvn -pl 04-flink-cluster-lifecycle compile exec:exec
```

Each folder has its own README with the teaching point, the improvement over the previous step, and one suggested tweak or complication to show live.

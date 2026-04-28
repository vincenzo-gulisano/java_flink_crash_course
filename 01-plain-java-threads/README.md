# 01 - Plain Java Threads

## What This Shows

This is a streaming pipeline without Flink. It has four stages connected by `BlockingQueue`s:

```text
producer thread -> parser thread -> filter thread -> aggregator thread
```

The data is a tiny stream of temperature readings, encoded as strings:

```text
room,timestampMillis,temperatureCelsius
```

The program parses each string, keeps only hot readings, and prints a running average per room.

## Improvement Target

This version makes the mechanics visible. Students can see that a stream is not "a big list"; it is data moving through stages over time.

## Tweak / Complication

The weakness is manual orchestration. Every thread must be started explicitly. If one stage is forgotten, the pipeline waits forever downstream.

Run the healthy version:

```bash
mvn -pl 01-plain-java-threads compile exec:exec
```

Run it while forgetting the parser thread:

```bash
mvn -pl 01-plain-java-threads compile exec:exec -DskipStage=parser
```

Good classroom prompt: "The producer is creating events, so why does the aggregator never print anything?"

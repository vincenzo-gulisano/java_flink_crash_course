# 03 - Flink With Tuples, Local Mode

## What This Shows

This version parses each raw event once into a Flink tuple:

```java
Tuple3<String, Long, Double>
```

The fields are:

```text
f0 = room
f1 = timestampMillis
f2 = temperatureCelsius
```

After that, the pipeline works with typed values instead of repeatedly splitting strings. It keeps hot readings, groups them by room, and prints the average of every three hot readings per room.

## Improvement Over Step 02

The code is less fragile. A temperature is a `Double`, a timestamp is a `Long`, and the parser is close to the source instead of scattered through the whole pipeline.

## Tweak / Complication

This runs locally in one JVM. That is excellent for development and debugging, but it hides some distributed-system details. The next step keeps nearly the same logic and discusses what changes when Flink ships work to TaskManagers.

Run it locally:

```bash
mvn -pl 03-flink-tuples-local compile exec:exec
```

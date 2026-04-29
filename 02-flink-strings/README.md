# 02 - Flink With Strings

## What This Shows

This version expresses the same streaming idea as a Flink job:

```text
source -> normalize -> assign event time -> filter -> sliding window -> average -> print
```

The important change is that the program describes the dataflow. Flink decides how to run the operators; we no longer manually create and start threads.

The query is the same one from step 01:

```text
current-time readings -> keep hot readings -> average by room in 20-second windows every 5 seconds
```

## Improvement Over Step 01

The pipeline cannot get stuck because we forgot to call `start()` on one stage. We define the transformations and call `env.execute(...)`.

## Tweak / Complication

The limitation is that every event is still a `String`. The code repeatedly relies on field positions:

```text
fields[0] = room
fields[1] = timestamp
fields[2] = temperature
```

Classroom tweak: in `temperatureFrom(...)`, change `fields(line)[2]` to `fields(line)[1]`. The code still compiles, but the job behavior is wrong or fails at runtime.

Another useful prompt: "Why do we have to call `assignTimestampsAndWatermarks(...)` before using event-time windows?"

Run it locally:

```bash
mvn -pl 02-flink-strings compile exec:exec
```

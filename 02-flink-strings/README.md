# 02 - Flink With Strings

## What This Shows

This version expresses the same streaming idea as a Flink job:

```text
source -> normalize -> assign event time -> filter -> sliding window -> average -> print
```

The important change is that the program describes the dataflow. Flink decides how to run the operators; we no longer manually create and start threads.

The query is the same one from step 01:

```text
current-time readings -> keep hot readings -> average by room in 5-second windows every 2 seconds
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

## Code Questions

Question: "Do you understand why there's a `keyBy` here?"

Answer: the `keyBy(FlinkStringPipeline::roomFrom)` tells Flink that readings with the same room name belong together. The sliding window is then computed separately for each room, so we get one average for `kitchen`, one for `office`, and so on.

Without the `keyBy`, Flink would see one stream of hot readings and the window would compute one average across all rooms. In a distributed Flink job, `keyBy` also decides where the data goes: all events with the same key are sent to the same parallel task, so that task can keep the correct window state for that room.

This example also shows the weakness of using strings: `roomFrom(...)` has to extract the key from a field position in the text. If the string format changes, or if we read the wrong field, the grouping becomes wrong even though the code may still compile.

Run it locally:

```bash
mvn -pl 02-flink-strings compile exec:exec
```

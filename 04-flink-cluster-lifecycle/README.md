# 04 - Cluster Execution And Lifecycle

## What This Shows

This keeps the tuple pipeline from step 03, but prepares it for the cluster discussion:

```text
client builds JobGraph -> JobManager coordinates -> TaskManagers run the operators
```

The important new idea is that Flink serializes user functions and ships them to workers. Runtime-only objects should be created in `open(...)` and cleaned in `close()`.

The query is still the same one from steps 01-03:

```text
current-time readings -> keep hot readings -> average by room in 20-second windows every 5 seconds
```

## Improvement Over Step 03

The job logic is almost the same as step 03, but the execution story changes. We can run locally for development, then submit the same class to a Flink cluster.

## Tweak / Complication

The class contains two formatter implementations:

- `LifecycleFormatter`: correct version. Serializable configuration goes in the constructor; runtime helper creation goes in `open(OpenContext)`; cleanup goes in `close()`.
- `BadConstructorFormatter`: deliberately wrong version. It creates a non-serializable helper in the constructor, which is a useful way to show why constructor initialization can break once Flink serializes the job.

Run the correct version locally:

```bash
mvn -pl 04-flink-cluster-lifecycle compile exec:exec
```

Run with more local parallelism:

```bash
mvn -pl 04-flink-cluster-lifecycle compile exec:exec -Dparallelism=2
```

Run the deliberately broken lifecycle version:

```bash
mvn -pl 04-flink-cluster-lifecycle compile exec:exec -DbadLifecycle=true
```

Package the jar:

```bash
mvn -pl 04-flink-cluster-lifecycle -am clean package
```

Submit it to a standalone Flink cluster:

```bash
$FLINK_HOME/bin/flink run \
  -c edu.streaming.step04.FlinkClusterLifecyclePipeline \
  04-flink-cluster-lifecycle/target/step-04-flink-cluster-lifecycle-1.0-SNAPSHOT.jar \
  --parallelism=2
```

Good classroom prompt: "Why is the constructor allowed to store `String label`, but not a live helper object or connection?"

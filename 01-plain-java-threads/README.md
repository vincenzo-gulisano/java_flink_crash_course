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

## Code Questions

**Question:** Do you know what `private static final` means for a variable?

**Answer:** `private` means the variable can only be used inside this class. `static` means there is one shared variable belonging to the class, not one copy per object. `final` means the variable cannot be reassigned after it is initialized. Together, `private static final` is commonly used for constants such as `END_OF_STREAM`.

**Question:** Why would we want to prevent instantiation of this class?

**Answer:** `ManualThreadPipeline` only contains a `main` method and helper methods. We do not need objects of this class, so a private constructor prevents someone from accidentally writing `new ManualThreadPipeline()`.

**Question:** Do you know what `<String>` means here?

**Answer:** `<String>` is a generic type parameter. It tells Java that this queue contains only `String` values. The compiler can then prevent us from accidentally putting another kind of object into the queue.

**Question:** Do you know why we use a `BlockingQueue` here instead of a regular `Queue` or `List`?

**Answer:** A `BlockingQueue` is designed for communication between threads. If a consumer calls `take()` and the queue is empty, it waits instead of failing or spinning in a loop. If a producer calls `put()`, the queue handles the thread-safe handoff.

**Question:** Do you know what `() -> produce(rawEvents)` means here?

**Answer:** This is a lambda expression. It means: "when this thread starts, run the method `produce(rawEvents)`." It is a compact way to pass behavior to the `Thread` constructor.

**Question:** Do you know what `private static` means for a method?

**Answer:** `private` means the method is only callable inside this class. `static` means the method belongs to the class itself, so we can call it without creating an object first.

**Question:** Do you know what `<String, RoomStats>` means here?

**Answer:** It describes the two generic types used by the `Map`. The key type is `String`, which is the room name. The value type is `RoomStats`, which stores the count and sum for that room.

**Question:** Do you know what `private static final` means for a class?

**Answer:** For the nested `Reading` class, `private` means it is only visible inside `ManualThreadPipeline`. `static` means a `Reading` object is independent from any `ManualThreadPipeline` object. That is exactly what we want: a reading is just data moving through the queues, not something that needs to remember a particular pipeline object.

If `Reading` were not `static`, Java would treat it as an inner class tied to an instance of `ManualThreadPipeline`. To create a `Reading`, we would first need an outer object, conceptually like this:

```java
ManualThreadPipeline pipeline = new ManualThreadPipeline();
Reading reading = pipeline.new Reading("lab", 1714300000000L, 22.5, false);
```

That would be awkward here because the constructor of `ManualThreadPipeline` is private and, more importantly, because the program is designed as a utility class with only static methods. A non-static inner `Reading` would also keep a hidden reference to the outer `ManualThreadPipeline` object. That extra hidden link is unnecessary and can be confusing when objects are passed between threads. `final` means no other class can extend `Reading`.

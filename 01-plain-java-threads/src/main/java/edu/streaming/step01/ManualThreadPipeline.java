package edu.streaming.step01;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Demonstrates a streaming pipeline built manually with Java threads and blocking queues.
 */
public final class ManualThreadPipeline {
    private static final String END_OF_STREAM = "__END__";
    private static final double HOT_THRESHOLD = 22.0;
    private static final int EVENT_COUNT = 18;
    private static final long JOIN_TIMEOUT_MILLIS = 1_500L;

    /**
     * Utility class: this private constructor prevents accidental instantiation.
     */
    private ManualThreadPipeline() {
    }

    /**
     * Builds the queues and threads, starts each pipeline stage, and reports whether any stage got stuck.
     */
    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<String> rawEvents = new LinkedBlockingQueue<>();
        BlockingQueue<Reading> parsedReadings = new LinkedBlockingQueue<>();
        BlockingQueue<Reading> hotReadings = new LinkedBlockingQueue<>();

        Thread producer = new Thread(() -> produce(rawEvents), "producer");
        Thread parser = new Thread(() -> parse(rawEvents, parsedReadings), "parser");
        Thread filter = new Thread(() -> filterHotReadings(parsedReadings, hotReadings), "filter");
        Thread aggregator = new Thread(() -> aggregateByRoom(hotReadings), "aggregator");

        List<Thread> pipeline = Arrays.asList(producer, parser, filter, aggregator);

        startUnlessSkipped("producer", producer, args);
        startUnlessSkipped("parser", parser, args);
        startUnlessSkipped("filter", filter, args);
        startUnlessSkipped("aggregator", aggregator, args);

        for (Thread thread : pipeline) {
            thread.join(JOIN_TIMEOUT_MILLIS);
        }

        boolean somethingIsStillWaiting = false;
        for (Thread thread : pipeline) {
            if (thread.isAlive()) {
                somethingIsStillWaiting = true;
                System.out.printf(
                        "%n%s is still waiting. Its current state is %s.%n",
                        thread.getName(),
                        thread.getState());
            }
        }

        if (somethingIsStillWaiting) {
            System.out.println("The pipeline did not finish. In this version, every stage must be started by hand.");
            for (Thread thread : pipeline) {
                thread.interrupt();
            }
        } else {
            System.out.println("\nAll stages finished.");
        }
    }

    /**
     * Starts one named stage unless the classroom demo disables it with a command-line flag.
     */
    private static void startUnlessSkipped(String stageName, Thread thread, String[] args) {
        if (wasSkipped(stageName, args)) {
            System.out.printf("Skipping %-10s because this stage was disabled for the demo.%n", stageName);
            return;
        }

        System.out.printf("Starting %-10s%n", stageName);
        thread.start();
    }

    /**
     * Checks both Maven-friendly system properties and direct command-line flags for skipped stages.
     */
    private static boolean wasSkipped(String stageName, String[] args) {
        if (stageName.equals(System.getProperty("skipStage", ""))) {
            return true;
        }

        String skipFlag = "--skip-" + stageName;
        for (String arg : args) {
            if (skipFlag.equals(arg)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Producer stage: creates raw CSV-like sensor events and places them on the first queue.
     */
    private static void produce(BlockingQueue<String> output) {
        try {
            for (int i = 1; i <= EVENT_COUNT; i++) {
                String event = sampleEvent(i);
                System.out.printf("[producer]   %s%n", event);
                output.put(event);
                Thread.sleep(40L);
            }
            output.put(END_OF_STREAM);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Parser stage: reads raw strings, converts them to {@link Reading} objects, and forwards them.
     */
    private static void parse(BlockingQueue<String> input, BlockingQueue<Reading> output) {
        try {
            while (true) {
                String line = input.take();
                if (END_OF_STREAM.equals(line)) {
                    output.put(Reading.end());
                    return;
                }

                Reading reading = Reading.fromCsv(line);
                System.out.printf("[parser]     %s%n", reading);
                output.put(reading);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Filter stage: keeps only readings at or above the temperature threshold.
     */
    private static void filterHotReadings(BlockingQueue<Reading> input, BlockingQueue<Reading> output) {
        try {
            while (true) {
                Reading reading = input.take();
                if (reading.endOfStream) {
                    output.put(reading);
                    return;
                }

                if (reading.temperatureCelsius >= HOT_THRESHOLD) {
                    System.out.printf("[filter]     keeping %s%n", reading);
                    output.put(reading);
                } else {
                    System.out.printf("[filter]     dropping %s%n", reading);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Aggregator stage: maintains a running count and average temperature for each room.
     */
    private static void aggregateByRoom(BlockingQueue<Reading> input) {
        Map<String, RoomStats> statsByRoom = new HashMap<>();

        try {
            while (true) {
                Reading reading = input.take();
                if (reading.endOfStream) {
                    return;
                }

                RoomStats stats = statsByRoom.computeIfAbsent(reading.room, ignored -> new RoomStats());
                stats.add(reading.temperatureCelsius);
                System.out.printf(
                        Locale.US,
                        "[aggregator] room=%s count=%d average=%.1f C%n",
                        reading.room,
                        stats.count,
                        stats.average());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Creates deterministic sample input so every run of the demo tells the same story.
     */
    private static String sampleEvent(int index) {
        String[] rooms = {"lab", "kitchen", "office"};
        String room = rooms[index % rooms.length];
        long timestampMillis = 1_714_300_000_000L + index * 1_000L;
        double temperature = 20.2 + (index % 5) * 0.7 + (index % 4 == 0 ? 2.0 : 0.0);
        return String.format(Locale.US, "%s,%d,%.1f", room, timestampMillis, temperature);
    }

    /**
     * A parsed temperature reading passed between pipeline stages after the parser.
     */
    private static final class Reading {
        private final String room;
        private final long timestampMillis;
        private final double temperatureCelsius;
        private final boolean endOfStream;

        /**
         * Creates either a real reading or a sentinel value used to signal the end of the stream.
         */
        private Reading(String room, long timestampMillis, double temperatureCelsius, boolean endOfStream) {
            this.room = room;
            this.timestampMillis = timestampMillis;
            this.temperatureCelsius = temperatureCelsius;
            this.endOfStream = endOfStream;
        }

        /**
         * Parses one raw CSV-like event into a structured reading.
         */
        private static Reading fromCsv(String line) {
            String[] fields = line.split(",");
            if (fields.length != 3) {
                throw new IllegalArgumentException("Expected room,timestamp,temperature but got: " + line);
            }
            return new Reading(
                    fields[0],
                    Long.parseLong(fields[1]),
                    Double.parseDouble(fields[2]),
                    false);
        }

        /**
         * Creates the sentinel object that tells downstream stages no more events are coming.
         */
        private static Reading end() {
            return new Reading("", 0L, 0.0, true);
        }

        /**
         * Formats the reading for the console output shown during the demo.
         */
        @Override
        public String toString() {
            return String.format(
                    Locale.US,
                    "%s at %d -> %.1f C",
                    room,
                    timestampMillis,
                    temperatureCelsius);
        }
    }

    /**
     * Mutable aggregation state for one room.
     */
    private static final class RoomStats {
        private long count;
        private double sum;

        /**
         * Adds one temperature reading to this room's running statistics.
         */
        private void add(double temperatureCelsius) {
            count++;
            sum += temperatureCelsius;
        }

        /**
         * Returns the current average temperature for this room.
         */
        private double average() {
            return sum / count;
        }
    }
}

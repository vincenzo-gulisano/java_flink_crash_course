package edu.streaming.step02;

import java.time.Duration;
import java.util.Locale;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
public final class FlinkStringPipeline {
    private static final double HOT_THRESHOLD = 22.0;
    private static final int EVENT_COUNT = 18;
    private static final long PRODUCER_SLEEP_MILLIS = 40L;
    private static final long WINDOW_SIZE_MILLIS = 20_000L;
    private static final long WINDOW_SLIDE_MILLIS = 5_000L;

    private FlinkStringPipeline() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        DataStream<String> rawEvents = env
                .fromSequence(1, EVENT_COUNT)
                .name("event-counter")
                .setParallelism(1)
                .map(FlinkStringPipeline::sampleEvent)
                .name("sensor-csv-source");

        rawEvents
                .map(FlinkStringPipeline::normalizeRoomName)
                .name("normalize-room-name")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<String>forMonotonousTimestamps()
                                .withTimestampAssigner((event, previousTimestamp) -> timestampFrom(event)))
                .name("use-event-timestamp")
                .filter(FlinkStringPipeline::isHot)
                .name("keep-hot-readings")
                .keyBy(FlinkStringPipeline::roomFrom)
                .window(SlidingEventTimeWindows.of(
                        Duration.ofMillis(WINDOW_SIZE_MILLIS),
                        Duration.ofMillis(WINDOW_SLIDE_MILLIS)))
                .process(new AverageStringWindow())
                .name("average-hot-readings-by-window")
                .print()
                .name("print-window-averages");

        env.execute("Step 02 - Flink pipeline with String events");
    }

    private static String normalizeRoomName(String line) {
        String[] fields = fields(line);
        fields[0] = fields[0].trim().toLowerCase(Locale.ROOT);
        return String.join(",", fields);
    }

    private static boolean isHot(String line) {
        return temperatureFrom(line) >= HOT_THRESHOLD;
    }

    private static String roomFrom(String line) {
        return fields(line)[0];
    }

    private static long timestampFrom(String line) {
        return Long.parseLong(fields(line)[1]);
    }

    private static double temperatureFrom(String line) {
        return Double.parseDouble(fields(line)[2]);
    }

    private static String[] fields(String line) {
        String[] fields = line.split(",");
        if (fields.length != 3) {
            throw new IllegalArgumentException("Expected room,timestamp,temperature but got: " + line);
        }
        return fields;
    }

    public static final class AverageStringWindow extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(
                String room,
                Context context,
                Iterable<String> readings,
                Collector<String> out) {
            long count = 0L;
            double sum = 0.0;

            for (String reading : readings) {
                count++;
                sum += temperatureFrom(reading);
            }

            out.collect(formatAverage(room, context.window().getStart(), context.window().getEnd(), count, sum / count));
        }
    }

    private static String sampleEvent(long index) {
        sleep(PRODUCER_SLEEP_MILLIS);
        String[] rooms = {"lab", "kitchen", "office"};
        String room = rooms[(int) (index % rooms.length)];
        long timestampMillis = System.currentTimeMillis();
        double temperature = 20.2 + (index % 5) * 0.7 + (index % 4 == 0 ? 2.0 : 0.0);
        return String.format(Locale.US, "%s,%d,%.1f", room, timestampMillis, temperature);
    }

    private static String formatAverage(
            String room,
            long windowStart,
            long windowEnd,
            long count,
            double averageTemperature) {
        return String.format(
                Locale.US,
                "room=%s window=[%s,%s) count=%d average=%.1f C",
                room,
                formatTimestamp(windowStart),
                formatTimestamp(windowEnd),
                count,
                averageTemperature);
    }

    private static String formatTimestamp(long timestampMillis) {
        return String.format(Locale.US, "%tT", timestampMillis);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while producing sample events", e);
        }
    }
}

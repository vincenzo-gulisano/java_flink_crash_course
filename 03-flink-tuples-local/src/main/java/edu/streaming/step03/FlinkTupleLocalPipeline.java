package edu.streaming.step03;

import java.time.Duration;
import java.util.Locale;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
public final class FlinkTupleLocalPipeline {
    private static final double HOT_THRESHOLD = 22.0;
    private static final int EVENT_COUNT = 18;
    private static final long PRODUCER_SLEEP_MILLIS = 40L;
    private static final long WINDOW_SIZE_MILLIS = 20_000L;
    private static final long WINDOW_SLIDE_MILLIS = 5_000L;

    private FlinkTupleLocalPipeline() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        DataStream<Tuple3<String, Long, Double>> readings = env
                .fromSequence(1, EVENT_COUNT)
                .name("event-counter")
                .setParallelism(1)
                .map(FlinkTupleLocalPipeline::sampleEvent)
                .name("sensor-csv-source")
                .map(new ParseReading())
                .name("parse-to-tuple")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Long, Double>>forMonotonousTimestamps()
                                .withTimestampAssigner((reading, previousTimestamp) -> reading.f1))
                .name("use-event-timestamp");

        readings
                .filter(reading -> reading.f2 >= HOT_THRESHOLD)
                .name("keep-hot-readings")
                .keyBy(reading -> reading.f0)
                .window(SlidingEventTimeWindows.of(
                        Duration.ofMillis(WINDOW_SIZE_MILLIS),
                        Duration.ofMillis(WINDOW_SLIDE_MILLIS)))
                .aggregate(new AverageTemperature(), new AddWindowInfo())
                .name("average-hot-readings-by-window")
                .map(FlinkTupleLocalPipeline::formatAverage)
                .name("format-average")
                .print()
                .name("print-averages");

        env.execute("Step 03 - Flink pipeline with tuples in local mode");
    }

    private static String formatAverage(Tuple5<String, Long, Long, Long, Double> average) {
        return String.format(
                Locale.US,
                "room=%s window=[%s,%s) count=%d average=%.1f C",
                average.f0,
                formatTimestamp(average.f1),
                formatTimestamp(average.f2),
                average.f3,
                average.f4);
    }

    public static final class ParseReading implements MapFunction<String, Tuple3<String, Long, Double>> {
        @Override
        public Tuple3<String, Long, Double> map(String line) {
            String[] fields = line.split(",");
            if (fields.length != 3) {
                throw new IllegalArgumentException("Expected room,timestamp,temperature but got: " + line);
            }

            return Tuple3.of(
                    fields[0].trim().toLowerCase(Locale.ROOT),
                    Long.parseLong(fields[1]),
                    Double.parseDouble(fields[2]));
        }
    }

    public static final class AverageTemperature
            implements AggregateFunction<
                    Tuple3<String, Long, Double>,
                    Tuple3<String, Long, Double>,
                    Tuple3<String, Long, Double>> {

        @Override
        public Tuple3<String, Long, Double> createAccumulator() {
            return Tuple3.of("", 0L, 0.0);
        }

        @Override
        public Tuple3<String, Long, Double> add(
                Tuple3<String, Long, Double> reading,
                Tuple3<String, Long, Double> accumulator) {
            accumulator.f0 = reading.f0;
            accumulator.f1 = accumulator.f1 + 1L;
            accumulator.f2 = accumulator.f2 + reading.f2;
            return accumulator;
        }

        @Override
        public Tuple3<String, Long, Double> getResult(Tuple3<String, Long, Double> accumulator) {
            return Tuple3.of(accumulator.f0, accumulator.f1, accumulator.f2 / accumulator.f1);
        }

        @Override
        public Tuple3<String, Long, Double> merge(
                Tuple3<String, Long, Double> left,
                Tuple3<String, Long, Double> right) {
            return Tuple3.of(left.f0.isEmpty() ? right.f0 : left.f0, left.f1 + right.f1, left.f2 + right.f2);
        }
    }

    public static final class AddWindowInfo
            extends ProcessWindowFunction<
                    Tuple3<String, Long, Double>,
                    Tuple5<String, Long, Long, Long, Double>,
                    String,
                    TimeWindow> {

        @Override
        public void process(
                String room,
                Context context,
                Iterable<Tuple3<String, Long, Double>> averages,
                Collector<Tuple5<String, Long, Long, Long, Double>> out) {
            Tuple3<String, Long, Double> average = averages.iterator().next();
            out.collect(Tuple5.of(room, context.window().getStart(), context.window().getEnd(), average.f1, average.f2));
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

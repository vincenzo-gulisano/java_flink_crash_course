package edu.streaming.step03;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SuppressWarnings("deprecation")
public final class FlinkTupleLocalPipeline {
    private static final double HOT_THRESHOLD = 22.0;

    private FlinkTupleLocalPipeline() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        DataStream<Tuple3<String, Long, Double>> readings = env
                .fromCollection(sampleEvents())
                .name("sensor-csv-source")
                .map(new ParseReading())
                .name("parse-to-tuple");

        readings
                .filter(reading -> reading.f2 >= HOT_THRESHOLD)
                .name("keep-hot-readings")
                .keyBy(reading -> reading.f0)
                .countWindow(3)
                .aggregate(new AverageTemperature())
                .name("average-every-three-hot-readings")
                .map(FlinkTupleLocalPipeline::formatAverage)
                .name("format-average")
                .print()
                .name("print-averages");

        env.execute("Step 03 - Flink pipeline with tuples in local mode");
    }

    private static String formatAverage(Tuple3<String, Long, Double> average) {
        return String.format(
                Locale.US,
                "room=%s average-of-%d-hot-readings=%.1f C",
                average.f0,
                average.f1,
                average.f2);
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

    private static List<String> sampleEvents() {
        return Arrays.asList(
                "Lab,1714300000000,21.2",
                "Kitchen,1714300001000,23.0",
                "Office,1714300002000,22.5",
                "Lab,1714300003000,24.1",
                "Studio,1714300004000,20.9",
                "Kitchen,1714300005000,24.0",
                "Office,1714300006000,23.2",
                "Lab,1714300007000,22.8",
                "Studio,1714300008000,22.4",
                "Kitchen,1714300009000,21.9",
                "Office,1714300010000,24.0",
                "Lab,1714300011000,23.7",
                "Studio,1714300012000,23.5",
                "Kitchen,1714300013000,25.0",
                "Office,1714300014000,21.0",
                "Studio,1714300015000,24.2");
    }
}

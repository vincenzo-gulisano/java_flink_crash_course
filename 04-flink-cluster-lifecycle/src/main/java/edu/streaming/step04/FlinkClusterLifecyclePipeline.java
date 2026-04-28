package edu.streaming.step04;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SuppressWarnings("deprecation")
public final class FlinkClusterLifecyclePipeline {
    private static final double HOT_THRESHOLD = 22.0;

    private FlinkClusterLifecyclePipeline() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(parallelismFrom(args));

        DataStream<Tuple3<String, Long, Double>> readings = env
                .fromCollection(sampleEvents())
                .name("sensor-csv-source")
                .map(new ParseReading())
                .name("parse-to-tuple");

        DataStream<Tuple3<String, Long, Double>> averages = readings
                .filter(reading -> reading.f2 >= HOT_THRESHOLD)
                .name("keep-hot-readings")
                .keyBy(reading -> reading.f0)
                .countWindow(3)
                .aggregate(new AverageTemperature())
                .name("average-every-three-hot-readings");

        if (hasFlag(args, "--bad-lifecycle")) {
            averages
                    .map(new BadConstructorFormatter())
                    .name("bad-constructor-formatter")
                    .print()
                    .name("print-bad-output");
        } else {
            averages
                    .map(new LifecycleFormatter("cluster-demo"))
                    .name("lifecycle-formatter")
                    .print()
                    .name("print-averages");
        }

        env.execute("Step 04 - Flink cluster lifecycle and serialization");
    }

    private static int parallelismFrom(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--parallelism=")) {
                return Integer.parseInt(arg.substring("--parallelism=".length()));
            }
        }
        return Integer.parseInt(System.getProperty("parallelism", "1"));
    }

    private static boolean hasFlag(String[] args, String flag) {
        if ("--bad-lifecycle".equals(flag) && Boolean.getBoolean("badLifecycle")) {
            return true;
        }

        for (String arg : args) {
            if (flag.equals(arg)) {
                return true;
            }
        }
        return false;
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

    public static final class LifecycleFormatter extends RichMapFunction<Tuple3<String, Long, Double>, String> {
        private final String label;
        private transient LocalFormatter formatter;

        public LifecycleFormatter(String label) {
            this.label = label;
            System.out.printf("[constructor] Stored serializable label='%s'.%n", label);
        }

        @Override
        public void open(OpenContext openContext) {
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            formatter = new LocalFormatter(label, subtaskIndex, parallelism);
            System.out.printf(
                    "[open] Created runtime helper on subtask %d/%d in thread %s.%n",
                    subtaskIndex + 1,
                    parallelism,
                    Thread.currentThread().getName());
        }

        @Override
        public String map(Tuple3<String, Long, Double> average) {
            if (formatter == null) {
                throw new IllegalStateException("Runtime helper was not initialized. Did open(...) run?");
            }
            return formatter.format(average);
        }

        @Override
        public void close() {
            System.out.printf(
                    "[close] Cleaning runtime helper on subtask %d.%n",
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() + 1);
            formatter = null;
        }
    }

    public static final class BadConstructorFormatter extends RichMapFunction<Tuple3<String, Long, Double>, String> {
        private final LocalFormatter formatter;

        public BadConstructorFormatter() {
            formatter = new LocalFormatter("bad-constructor", -1, -1);
            System.out.println("[constructor] Created a non-serializable helper before Flink ships the function.");
        }

        @Override
        public String map(Tuple3<String, Long, Double> average) {
            return formatter.format(average);
        }
    }

    private static final class LocalFormatter {
        private final String label;
        private final int subtaskIndex;
        private final int parallelism;
        private final String threadName;

        private LocalFormatter(String label, int subtaskIndex, int parallelism) {
            this.label = label;
            this.subtaskIndex = subtaskIndex;
            this.parallelism = parallelism;
            this.threadName = Thread.currentThread().getName();
        }

        private String format(Tuple3<String, Long, Double> average) {
            return String.format(
                    Locale.US,
                    "%s subtask=%d/%d thread=%s room=%s average-of-%d-hot-readings=%.1f C",
                    label,
                    subtaskIndex + 1,
                    parallelism,
                    threadName,
                    average.f0,
                    average.f1,
                    average.f2);
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

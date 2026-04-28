package edu.streaming.step02;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SuppressWarnings("deprecation")
public final class FlinkStringPipeline {
    private static final double HOT_THRESHOLD = 22.0;

    private FlinkStringPipeline() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        DataStream<String> rawEvents = env
                .fromCollection(sampleEvents())
                .name("sensor-csv-source");

        rawEvents
                .map(FlinkStringPipeline::normalizeRoomName)
                .name("normalize-room-name")
                .filter(FlinkStringPipeline::isHot)
                .name("keep-hot-readings")
                .map(FlinkStringPipeline::formatAlert)
                .name("format-alert")
                .print()
                .name("print-alerts");

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

    private static String formatAlert(String line) {
        return String.format(
                Locale.US,
                "HOT room=%s timestamp=%d temperature=%.1f C",
                roomFrom(line),
                timestampFrom(line),
                temperatureFrom(line));
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

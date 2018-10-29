package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;

import static java.lang.Math.abs;

public class AvgSpeedStream implements Serializable {
    private transient DataStream<CarRecord> in;
    private String outputFilePath;

    private int speedLimit;

    private short segBegin = 52;
    private short segEnd = 56;

    public AvgSpeedStream(DataStream<CarRecord> carRecordDataStream, int speedLimit, String outputFile2) {
        this.in = carRecordDataStream;
        this.outputFilePath = outputFile2;
        this.speedLimit = speedLimit;

        this.run();
    }

    public AvgSpeedStream() {

    }

    private void run() {
        DataStream<AvgSpeedRecord> out = in.filter(new FilterFunction<CarRecord>() {
            @Override
            public boolean filter(CarRecord value) throws Exception {
                return value.seg >= segBegin && value.seg <= segEnd;
            }
        }).assignTimestamps(new TimestampExtractor<CarRecord>() {
            @Override
            public long extractTimestamp(CarRecord element, long currentTimestamp) {
                return element.time;
            }

            @Override
            public long extractWatermark(CarRecord element, long currentTimestamp) {
                return 0;
            }

            @Override
            public long getCurrentWatermark() {
                return 0;
            }
        }).keyBy("vid", "xway", "dir").window(EventTimeSessionWindows.withGap(Time.seconds(60))).apply(new WindowFunction<CarRecord, AvgSpeedRecord, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<CarRecord> input, Collector<AvgSpeedRecord> out) throws Exception {
                CarRecord first = input.iterator().next();
                CarRecord last = input.iterator().next();
                for (CarRecord cr : input){
                    if (cr.pos < first.pos){
                        first = cr;
                    }
                    if (cr.pos > last.pos){
                        last = cr;
                    }
                }
                int finalSpeed = calculateSpeed(first,last);
                if (first.seg == segBegin && last.seg == segEnd && finalSpeed > speedLimit){
                    out.collect(new AvgSpeedRecord(first, last, finalSpeed));
                }
            }
        });

        out.writeAsText(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    private int calculateSpeed(CarRecord first, CarRecord last) {
        return (int) ((abs(first.pos - last.pos) / 1609.344) / (abs(first.time - last.time)/3600.));
    }

    private class AvgSpeedRecord {
        private final int time1;
        private final int time2;
        private final String vid;
        private final String xway;
        private final short dir;
        private final short avgSpd;

        public AvgSpeedRecord(CarRecord first, CarRecord last, int finalSpd) {
            // Format: Time1, Time2, VID, XWay, Dir, AvgSpd,
            this.vid = first.vid;
            this.xway = first.xway;
            this.dir = first.dir;
            this.avgSpd = (short) finalSpd;
            if (first.dir == 0){
                time1 = first.time;
                time2 = last.time;
            }
            else{
                time1 = last.time;
                time2 = first.time;
            }
        }

        @Override
        public String toString() {
            return time1 +
                    "," + time2 +
                    "," + vid +
                    "," + xway +
                    "," + dir +
                    "," + avgSpd;
        }
    }

    public static void main(String[] args) {
        AvgSpeedStream a = new AvgSpeedStream();
        CarRecord c = new CarRecord();
        c.time = 0;
        c.pos = 0;
        CarRecord c1 = new CarRecord();
        c1.time = 3600;
        c1.pos = 96561;
        System.out.println(a.calculateSpeed(c, c1));
    }
}

package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
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

    private int segBegin;
    private int segEnd;

    public AvgSpeedStream(DataStream<CarRecord> carRecordDataStream, int speedLimit, int segBegin, int segEnd, String outputFile2) {
        this.in = carRecordDataStream;
        this.outputFilePath = outputFile2;
        this.speedLimit = speedLimit;
        this.segBegin = segBegin;
        this.segEnd = segEnd;

        this.run();
    }

    public AvgSpeedStream() {

    }

    private void run() {
        DataStream<AvgSpeedRecord> out = in.filter(new FilterFunction<CarRecord>() {
            @Override
            public boolean filter(CarRecord value) throws Exception {
                return value.getSeg() >= segBegin && value.getSeg() <= segEnd;
            }
        }).assignTimestamps(new TimestampExtractor<CarRecord>() {
            @Override
            public long extractTimestamp(CarRecord element, long currentTimestamp) {
                return element.getTime();
            }

            @Override
            public long extractWatermark(CarRecord element, long currentTimestamp) {
                return 0;
            }

            @Override
            public long getCurrentWatermark() {
                return 0;
            }
        }).keyBy(1,3,4).window(EventTimeSessionWindows.withGap(Time.seconds(60))).apply(new WindowFunction<CarRecord, AvgSpeedRecord, Tuple, TimeWindow>() {
            AvgSpeedRecord avgSpeedRecord = new AvgSpeedRecord();

            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<CarRecord> input, Collector<AvgSpeedRecord> out) throws Exception {
                CarRecord first = input.iterator().next();
                CarRecord last = input.iterator().next();
                for (CarRecord cr : input){
                    if (cr.getPos() < first.getPos()){
                        first = cr;
                    }
                    if (cr.getPos() > last.getPos()){
                        last = cr;
                    }
                }
                int finalSpeed = calculateSpeed(first,last);
                if (first.getSeg() == segBegin && last.getSeg() == segEnd && finalSpeed > speedLimit){
                    avgSpeedRecord.load(first, last, finalSpeed);
                    out.collect(avgSpeedRecord);
                }
            }
        });

        out.writeAsCsv(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    private int calculateSpeed(CarRecord first, CarRecord last) {
        return (int) ((abs(first.getPos() - last.getPos()) / 1609.344) / (abs(first.getTime() - last.getTime())/3600.));
    }

}

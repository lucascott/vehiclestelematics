package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;

import static java.lang.Math.abs;

public class AvgSpeedStream implements Serializable {
    private transient DataStream<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>> in;
    private String outputFilePath;
    private double convFactor = 2.236936292054402;

    private int speedLimit;
    private int segBegin;
    private int segEnd;

    public AvgSpeedStream(DataStream<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>> carRecordDataStream, int speedLimit, int segBegin, int segEnd, String outputFile2) {
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
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Short, Double>> out = in.filter(new FilterFunction<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>>() {
            @Override
            public boolean filter(Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> value) {
                return value.f5 >= segBegin && value.f5 <= segEnd;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> element) {
                return element.f0 * 1000;
            }
        }).keyBy(1, 3, 4).window(EventTimeSessionWindows.withGap(Time.seconds(31))).apply(new WindowFunction<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>, Tuple6<Integer, Integer, Integer, Integer, Short, Double>, Tuple, TimeWindow>() {
            Tuple6<Integer, Integer, Integer, Integer, Short, Double> avgSpeedRecord = new Tuple6<Integer, Integer, Integer, Integer, Short, Double>();

            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>> input, Collector<Tuple6<Integer, Integer, Integer, Integer, Short, Double>> out) {
                Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> first = input.iterator().next();
                Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> last = first;
                for (Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> cr : input) {
                    if (cr.f6 < first.f6) {
                        first = cr;
                    }
                    if (cr.f6 > last.f6) {
                        last = cr;
                    }
                }
                double finalSpeed = calculateSpeed(first.f6, first.f0, last.f6, last.f0);
                if (first.f5 == segBegin && last.f5 == segEnd && finalSpeed > speedLimit) {
                    // Time1, Time2, VID, XWay, Dir, AvgSpd
                    //   0      1     2    3     4     5
                    if (first.f4  == 0){
                        avgSpeedRecord.f0 = first.f0;
                        avgSpeedRecord.f1 = last.f0;
                    }
                    else {
                        avgSpeedRecord.f0 = last.f0;
                        avgSpeedRecord.f1 = first.f0;
                    }
                    avgSpeedRecord.f2 = first.f1;
                    avgSpeedRecord.f3 = first.f3;
                    avgSpeedRecord.f4 = first.f4;
                    avgSpeedRecord.f5 = finalSpeed;
                    out.collect(avgSpeedRecord);
                }
            }
        });

        out.writeAsCsv(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }
    // Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
    //  0     1    2    3           4    5    6
    private double calculateSpeed(int fPos, int fTime, int lPos, int lTime ) {
        return (lPos - fPos) * 1.0 / abs(lTime - fTime) * convFactor;
    }

}

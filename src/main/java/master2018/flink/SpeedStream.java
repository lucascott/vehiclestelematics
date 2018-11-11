package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.io.Serializable;

public class SpeedStream implements Serializable {
    private final transient DataStream<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>> in;
    private final String outputFilePath;
    private final int speedLimit;

    public SpeedStream(DataStream<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>> carRecordDataStream, int speedLimit, String outputFile1) {
        this.in = carRecordDataStream;
        this.outputFilePath = outputFile1;
        this.speedLimit = speedLimit;

        this.run();
    }
    // Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
    //  0     1    2    3           4    5    6
    private void run() {
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Short, Short, Short>> out = in.filter(new FilterFunction<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>>() {
            @Override
            public boolean filter(Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> value) {
                return value.f2 > speedLimit;
            }
        }).map(new MapFunction<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>, Tuple6<Integer, Integer, Integer, Short, Short, Short>>() {
            Tuple6<Integer, Integer, Integer, Short, Short, Short> speedRecord = new Tuple6<Integer, Integer, Integer, Short, Short, Short>();

            @Override
            public Tuple6<Integer, Integer, Integer, Short, Short, Short> map(Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> value) {
                // Time, VID, XWay, Seg, Dir, Spd
                speedRecord.f0 = value.f0;
                speedRecord.f1 = value.f1;
                speedRecord.f2 = value.f3;
                speedRecord.f3 = value.f5;
                speedRecord.f4 = value.f4;
                speedRecord.f5 = value.f2;
                return speedRecord;
            }
        });

        out.writeAsCsv(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Speed Stream");
    }
}

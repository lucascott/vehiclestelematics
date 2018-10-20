package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class AvgSpeedStream implements Serializable {
    private final transient DataStream<CarRecord> in;
    private final String outputFilePath;
    private final float speedLimit;
    private final short segBegin = 53;
    private final short segEnd = 56;

    public AvgSpeedStream(DataStream<CarRecord> carRecordDataStream, float speedLimit, String outputFile2) {
        this.in = carRecordDataStream;
        this.outputFilePath = outputFile2;
        this.speedLimit = speedLimit;

        this.run();
    }

    private void run() {
        SingleOutputStreamOperator<AvgSpeedRecord> out = in.keyBy(new KeySelector<CarRecord, Tuple3<String,String,Short>>() {
            @Override
            public Tuple3<String,String,Short> getKey(CarRecord value) throws Exception {
                return Tuple3.of(value.vid, value.xway, value.dir);
            }
        }).window(GlobalWindows.create())
                .apply(new WindowFunction<CarRecord, AvgSpeedRecord, Tuple3<String, String, Short>, GlobalWindow>() {
            @Override
            public void apply(Tuple3<String, String, Short> stringStringShortTuple3, GlobalWindow window, Iterable<CarRecord> input, Collector<AvgSpeedRecord> out) throws Exception {
                // TODO: CODE HERE
            }
        });

        out.writeAsText(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    private class AvgSpeedRecord {
        private final int time;
        private final String vid;
        private final String xway;
        private final short seg;
        private final short dir;
        private final float spd;

        public AvgSpeedRecord(CarRecord value) {
            // Format: Time, VID, XWay, Seg, Dir, Spd
            this.time = value.time;
            this.vid = value.vid;
            this.xway = value.xway;
            this.seg = value.seg;
            this.dir = value.dir;
            this.spd = value.spd;
        }

        @Override
        public String toString() {
            return time +
                    "," + vid +
                    "," + xway +
                    "," + seg +
                    "," + dir +
                    "," + spd;
        }
    }
}

package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class SpeedStream implements Serializable {
    private final transient DataStream<CarRecord> in;
    private final String outputFilePath;
    private final float speedLimit;

    public SpeedStream(DataStream<CarRecord> carRecordDataStream, float speedLimit, String outputFile1) {
        this.in = carRecordDataStream;
        this.outputFilePath = outputFile1;
        this.speedLimit = speedLimit;

        this.run();
    }

    private void run() {
        DataStream<SpeedRecord> out = in.filter(new FilterFunction<CarRecord>() {
            @Override
            public boolean filter(CarRecord value) throws Exception {
                return value.spd > speedLimit;
            }
        }).map(new MapFunction<CarRecord, SpeedRecord>() {
            @Override
            public SpeedRecord map(CarRecord value) throws Exception {
                return new SpeedRecord(value);
            }
        });

        out.writeAsText(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    private class SpeedRecord {
        private final int time;
        private final String vid;
        private final String xway;
        private final short seg;
        private final short dir;
        private final float spd;

        public SpeedRecord(CarRecord value) {
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

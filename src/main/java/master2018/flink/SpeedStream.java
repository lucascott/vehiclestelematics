package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class SpeedStream implements Serializable {
    private final transient DataStream<CarRecord> in;
    private final String outputFilePath;
    private final int speedLimit;

    public SpeedStream(DataStream<CarRecord> carRecordDataStream, int speedLimit, String outputFile1) {
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
            SpeedRecord speedRecord = new SpeedRecord();
            @Override
            public SpeedRecord map(CarRecord value) throws Exception {
                speedRecord.load(value);
                return speedRecord;
            }
        });

        out.writeAsText(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    private class SpeedRecord implements java.io.Serializable {
        private int time;
        private String vid;
        private String xway;
        private short seg;
        private short dir;
        private short spd;

        public SpeedRecord(CarRecord value) {
            // Format: Time, VID, XWay, Seg, Dir, Spd
            this.time = value.time;
            this.vid = value.vid;
            this.xway = value.xway;
            this.seg = value.seg;
            this.dir = value.dir;
            this.spd = value.spd;
        }

        public SpeedRecord() {

        }

        public void load(CarRecord value) {
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

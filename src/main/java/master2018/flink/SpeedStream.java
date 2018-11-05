package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

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
        SingleOutputStreamOperator<SpeedRecord> out = in.filter(new FilterFunction<CarRecord>() {
            @Override
            public boolean filter(CarRecord value) {
                return value.getSpd() > speedLimit;
            }
        }).map(new MapFunction<CarRecord, SpeedRecord>() {
            SpeedRecord speedRecord = new SpeedRecord();

            @Override
            public SpeedRecord map(CarRecord value) {
                speedRecord.load(value);
                return speedRecord;
            }
        });

        out.writeAsCsv(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }
}

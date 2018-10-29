package master2018.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class VehicleTelematics {
    public static void main(String[] args) {

        String inputPath = args[0];
        String outputPath = args[1];

        String outputFile1 = outputPath + "/speedfines.csv";
        String outputFile2 = outputPath + "/avgspeedfines.csv";
        String outputFile3 = outputPath + "/accidents.csv";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> input = env.readTextFile(inputPath).setParallelism(1);
        DataStream<CarRecord> carRecordDataStream = input.map(new MapFunction<String, CarRecord>() {
            @Override
            public CarRecord map(String value) throws Exception {
                String[] arr = value.split(",");
                return new CarRecord(arr);
            }
        }).setParallelism(1);

        new SpeedStream(carRecordDataStream,90, outputFile1);
        new AvgSpeedStream(carRecordDataStream,60, outputFile2);
        new AccidentStream(carRecordDataStream, outputFile3);

        try {
            env.execute("VehicleTelematics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

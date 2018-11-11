package master2018.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
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
        DataStream<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>> carRecordDataStream = input.map(new MapFunction<String, Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>>() {
            Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> carRecord = new Tuple7<>();

            @Override
            public Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> map(String value) {
                String[] arr = value.split(",");
                carRecord.f0 = Integer.parseInt(arr[0]);
                carRecord.f1 = Integer.parseInt(arr[1]);
                carRecord.f2 = Short.parseShort(arr[2]);
                carRecord.f3 = Integer.parseInt(arr[3]);
                carRecord.f4 = Short.parseShort(arr[5]);
                carRecord.f5 = Short.parseShort(arr[6]);
                carRecord.f6 = Integer.parseInt(arr[7]);
                return carRecord;
            }
        }).setParallelism(1);

        new AccidentStream(carRecordDataStream, outputFile3);
        new AvgSpeedStream(carRecordDataStream, 60, 52, 56, outputFile2);
        new SpeedStream(carRecordDataStream, 90, outputFile1);

        try {
            env.execute("VehicleTelematics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

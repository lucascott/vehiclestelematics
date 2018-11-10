package master2018.flink;

import master2018.flink.records.AccidentRecord;
import master2018.flink.records.CarRecord;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class AccidentStream implements Serializable {
    private final transient DataStream<CarRecord> in;
    private final String outputFilePath;

    public AccidentStream(DataStream<CarRecord> carRecordDataStream, String outputFile3) {
        this.in = carRecordDataStream;
        this.outputFilePath = outputFile3;

        this.run();
    }

    private void run() {
        SingleOutputStreamOperator<AccidentRecord> out = in.filter(new FilterFunction<CarRecord>() {
            @Override
            public boolean filter(CarRecord value) throws Exception {
                return value.getSpd() == 0;
            }
        }).keyBy(1,3,4,6).countWindow(4,1).apply(new WindowFunction<CarRecord, AccidentRecord, Tuple, GlobalWindow>() {
            AccidentRecord accidentRecord = new AccidentRecord();

            @Override
            public void apply(Tuple tuple, GlobalWindow window, Iterable<CarRecord> input, Collector<AccidentRecord> out) {
                short count = 0;
                CarRecord f = input.iterator().next();
                for (CarRecord cr : input) {
                    count++;
                    if (count == 4) {
                        accidentRecord.load(f.getTime(), cr.getTime(), cr.getVid(), cr.getXway(), cr.getSeg(), cr.getDir(), cr.getPos());
                        out.collect(accidentRecord);
                    }
                }
            }
        });

        out.writeAsCsv(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Accident Stream");
    }
}
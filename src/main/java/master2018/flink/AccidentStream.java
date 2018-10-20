package master2018.flink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
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
        DataStream<AccidentRecord> out = in.keyBy(new KeySelector<CarRecord, String>() {
            @Override
            public String getKey(CarRecord value) throws Exception {
                return value.vid;
            }
        }).countWindow(4,1).apply(new WindowFunction<CarRecord, AccidentRecord, String, GlobalWindow>() {
            @Override
            public void apply(String s, GlobalWindow window, Iterable<CarRecord> input, Collector<AccidentRecord> out) throws Exception {
                String str ="";
                for (CarRecord cr:input
                     ) {
                    str += cr.toString() + "\n";
                }
                str += "##########";
                out.collect(new AccidentRecord(str));
            }
        });

        out.writeAsText(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    private class AccidentRecord {
        String s;

        public AccidentRecord(String str) {
            this.s = str;
        }

        @Override
        public String toString() {
            return s;
        }
    }
}

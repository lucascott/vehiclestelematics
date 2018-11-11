package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class AccidentStream implements Serializable {
    private final transient DataStream<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>> in;
    private final String outputFilePath;

    public AccidentStream(DataStream<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>> carRecordDataStream, String outputFile3) {
        this.in = carRecordDataStream;
        this.outputFilePath = outputFile3;

        this.run();
    }

    private void run() {
        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Short, Short, Integer>> out = in.filter(new FilterFunction<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>>() {
            @Override
            public boolean filter(Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> value) throws Exception {
                return value.f2 == 0;
            }
        }).setParallelism(1).keyBy(1,3,4,6).countWindow(4,1).apply(new WindowFunction<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>, Tuple7<Integer, Integer, Integer, Integer, Short, Short, Integer>, Tuple, GlobalWindow>() {
            Tuple7<Integer, Integer, Integer, Integer, Short, Short, Integer> accidentRecord = new Tuple7<Integer, Integer, Integer, Integer, Short, Short, Integer>();

            @Override
            public void apply(Tuple tuple, GlobalWindow window, Iterable<Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer>> input, Collector<Tuple7<Integer, Integer, Integer, Integer, Short, Short, Integer>> out) {
                short count = 0;
                Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> f = input.iterator().next();
                for (Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> cr : input) {
                    count++;
                    if (count == 4) {
                        // Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
                        //  0     1    2    3           4    5    6
                        //accidentRecord.load(f.getTime(), cr.getTime(), cr.getVid(), cr.getXway(), cr.getSeg(), cr.getDir(), cr.getPos());
                        accidentRecord.f0 = f.f0;
                        accidentRecord.f1 = cr.f0;
                        accidentRecord.f2 = cr.f1;
                        accidentRecord.f3 = cr.f3;
                        accidentRecord.f4 = cr.f5;
                        accidentRecord.f5 = cr.f4;
                        accidentRecord.f6 = cr.f6;

                        out.collect(accidentRecord);
                    }
                }
            }
        });

        out.writeAsCsv(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("Accident Stream");
    }
}
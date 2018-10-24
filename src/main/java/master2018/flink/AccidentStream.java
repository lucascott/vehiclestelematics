package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple;
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
        DataStream<AccidentRecord> out = in.keyBy("vid","xway","dir").countWindow(4,1).apply(new WindowFunction<CarRecord, AccidentRecord, Tuple, GlobalWindow>() {
            @Override
            public void apply(Tuple tuple, GlobalWindow window, Iterable<CarRecord> input, Collector<AccidentRecord> out) throws Exception {
                short count = 0;
                CarRecord first = null;
                CarRecord last = null;
                for (CarRecord cr:input) {
                    count++;
                    if (count == 1){
                        first = cr;
                    }
                    if (count == 4){
                        last = cr;
                    }
                    if (cr.pos != first.pos){
                        break;
                    }
                }
                if(count == 4) {
                    out.collect(new AccidentRecord(first.time, last.time, first.vid, first.xway, first.seg, first.dir, first.pos));
                }
            }
        });

        out.writeAsText(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    private class AccidentRecord {

        private final int timeBegin;
        private final int timeEnd;
        private final String vid;
        private final String xway;
        private final short seg;
        private final short dir;
        private final int pos;

        AccidentRecord(int timeBegin, int timeEnd, String vid, String xway, short seg, short dir, int pos) {
            this.timeBegin = timeBegin;
            this.timeEnd = timeEnd;
            this.vid = vid;
            this.xway = xway;
            this.seg = seg;
            this.dir = dir;
            this.pos = pos;
        }

        @Override
        public String toString() {
            return timeBegin +
                    "," + timeEnd +
                    "," + vid +
                    "," + xway +
                    "," + seg +
                    "," + dir +
                    "," + pos;
        }
    }
}
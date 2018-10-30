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
            AccidentRecord accidentRecord = new AccidentRecord();
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
                    accidentRecord.load(first.time, last.time, first.vid, first.xway, first.seg, first.dir, first.pos);
                    out.collect(accidentRecord);
                }
            }
        });

        out.writeAsText(outputFilePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

    private class AccidentRecord implements java.io.Serializable {

        private int timeBegin;
        private int timeEnd;
        private String vid;
        private String xway;
        private short seg;
        private short dir;
        private int pos;

        public AccidentRecord(int timeBegin, int timeEnd, String vid, String xway, short seg, short dir, int pos) {
            this.timeBegin = timeBegin;
            this.timeEnd = timeEnd;
            this.vid = vid;
            this.xway = xway;
            this.seg = seg;
            this.dir = dir;
            this.pos = pos;
        }

        public AccidentRecord() {

        }

        public void load(int timeBegin, int timeEnd, String vid, String xway, short seg, short dir, int pos) {
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
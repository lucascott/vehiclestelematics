package master2018.flink.records;

import org.apache.flink.api.java.tuple.Tuple7;

public class AccidentRecord extends Tuple7<Integer, Integer, Integer, Integer, Short, Short, Integer> {

    public AccidentRecord(int timeBegin, int timeEnd, int vid, int xway, short seg, short dir, int pos) {
        super(timeBegin, timeEnd, vid, xway, seg, dir, pos);
    }

    public AccidentRecord() {

    }

    public void load(int timeBegin, int timeEnd, int vid, int xway, short seg, short dir, int pos) {
        this.setTimeBegin(timeBegin);
        this.setTimeEnd(timeEnd);
        this.setVid(vid);
        this.setXway(xway);
        this.setSeg(seg);
        this.setDir(dir);
        this.setPos(pos);
    }

    public int getTimeBegin() {
        return f0;
    }

    public void setTimeBegin(int timeBegin) {
        this.f0 = timeBegin;
    }

    public int getTimeEnd() {
        return f1;
    }

    public void setTimeEnd(int timeEnd) {
        this.f1 = timeEnd;
    }

    public int getVid() {
        return f2;
    }

    public void setVid(int vid) {
        this.f2 = vid;
    }

    public int getXway() {
        return f3;
    }

    public void setXway(int xway) {
        this.f3 = xway;
    }

    public short getSeg() {
        return f4;
    }

    public void setSeg(short seg) {
        this.f4 = seg;
    }

    public short getDir() {
        return f5;
    }

    public void setDir(short dir) {
        this.f5 = dir;
    }

    public int getPos() {
        return f6;
    }

    public void setPos(int pos) {
        this.f6 = pos;
    }
}

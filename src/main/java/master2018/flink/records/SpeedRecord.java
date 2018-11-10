package master2018.flink.records;


import org.apache.flink.api.java.tuple.Tuple6;

public class SpeedRecord extends Tuple6<Integer, Integer, Integer, Short, Short, Short> {

    public SpeedRecord() {

    }

    public void load(CarRecord value) {
        // Format: Time, VID, XWay, Seg, Dir, Spd
        this.setTime(value.getTime());
        this.setVid(value.getVid());
        this.setXway(value.getXway());
        this.setSeg(value.getSeg());
        this.setDir(value.getDir());
        this.setSpd(value.getSpd());
    }

    public int getTime() {
        return f0;
    }

    public void setTime(int time) {
        this.f0 = time;
    }

    public int getVid() {
        return f1;
    }

    public void setVid(int vid) {
        this.f1 = vid;
    }

    public int getXway() {
        return f2;
    }

    public void setXway(int xway) {
        this.f2 = xway;
    }

    public short getSeg() {
        return f3;
    }

    public void setSeg(short seg) {
        this.f3 = seg;
    }

    public short getDir() {
        return f4;
    }

    public void setDir(short dir) {
        this.f4 = dir;
    }

    public short getSpd() {
        return f5;
    }

    public void setSpd(short spd) {
        this.f5 = spd;
    }
}

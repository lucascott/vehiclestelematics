package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple7;

public class CarRecord extends Tuple7<Integer, Integer, Short, Integer, Short, Short, Integer> {
    // Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
    //  0     1    2    3           4    5    6

    public CarRecord() {
    }

    public CarRecord(String[] arr) {
        super(Integer.parseInt(arr[0]),
                Integer.parseInt(arr[1]),
                Short.parseShort(arr[2]),
                Integer.parseInt(arr[3]),
                Short.parseShort(arr[5]),
                Short.parseShort(arr[6]),
                Integer.parseInt(arr[7]));
    }

    public void load(String[] arr) {
        this.f0 = Integer.parseInt(arr[0]);
        this.f1 = Integer.parseInt(arr[1]);
        this.f2 = Short.parseShort(arr[2]);
        this.f3 = Integer.parseInt(arr[3]);
        this.f4 = Short.parseShort(arr[5]);
        this.f5 = Short.parseShort(arr[6]);
        this.f6 = Integer.parseInt(arr[7]);
    }

    @Override
    public String toString() {
        return "CarRecord{" +
                "time=" + f0 +
                ", vid='" + f1 +
                ", spd=" + f2 +
                ", xway='" + f3 +
                ", dir=" + f4 +
                ", seg=" + f5 +
                ", pos=" + f6 +
                '}';
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

    public short getSpd() {
        return f2;
    }

    public void setSpd(short spd) {
        this.f2 = spd;
    }

    public int getXway() {
        return f3;
    }

    public void setXway(int xway) {
        this.f3 = xway;
    }

    public short getDir() {
        return f4;
    }

    public void setDir(short dir) {
        this.f4 = dir;
    }

    public short getSeg() {
        return f5;
    }

    public void setSeg(short seg) {
        this.f5 = seg;
    }

    public int getPos() {
        return f6;
    }

    public void setPos(int pos) {
        this.f6 = pos;
    }
}

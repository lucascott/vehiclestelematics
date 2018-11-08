package master2018.flink;

import org.apache.flink.api.java.tuple.Tuple6;

public class AvgSpeedRecord extends Tuple6<Integer, Integer, Integer, Integer, Short, Double> {

    public AvgSpeedRecord() {
    }

    public void load(CarRecord first, CarRecord last, double finalSpd) {
        // Format: Time1, Time2, VID, XWay, Dir, AvgSpd
        this.setVid(first.getVid());
        this.setXway(first.getXway());
        this.setDir(first.getDir());
        this.setAvgSpd(finalSpd);
        if (first.getDir() == 0) {
            this.setTime1(first.getTime());
            this.setTime2(last.getTime());
        } else {
            this.setTime1(last.getTime());
            this.setTime2(first.getTime());
        }
    }

    public int getTime1() {
        return f0;
    }

    public void setTime1(int time1) {
        this.f0 = time1;
    }

    public int getTime2() {
        return f1;
    }

    public void setTime2(int time2) {
        this.f1 = time2;
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

    public short getDir() {
        return f4;
    }

    public void setDir(short dir) {
        this.f4 = dir;
    }

    public double getAvgSpd() {
        return f5;
    }

    public void setAvgSpd(double avgSpd) {
        this.f5 = avgSpd;
    }
}

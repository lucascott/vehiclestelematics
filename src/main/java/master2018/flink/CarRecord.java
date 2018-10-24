package master2018.flink;

public class CarRecord {
    int time;
    String vid;
    short spd;
    String xway;
    short lane;
    short dir;
    short seg;
    int pos;

    public CarRecord() {
    }

    public CarRecord(String[] arr) {
        this.time = Integer.parseInt(arr[0]);
        this.vid = arr[1];
        this.spd = Short.parseShort(arr[2]);
        this.xway = arr[3];
        this.lane = Short.parseShort(arr[4]);
        this.dir = Short.parseShort(arr[5]);
        this.seg = Short.parseShort(arr[6]);
        this.pos = Integer.parseInt(arr[7]);
    }

    @Override
    public String toString() {
        return "CarRecord{" +
                "time=" + time +
                ", vid='" + vid + '\'' +
                ", spd=" + spd +
                ", xway='" + xway + '\'' +
                ", lane=" + lane +
                ", dir=" + dir +
                ", seg=" + seg +
                ", pos=" + pos +
                '}';
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public short getSpd() {
        return spd;
    }

    public void setSpd(short spd) {
        this.spd = spd;
    }

    public String getXway() {
        return xway;
    }

    public void setXway(String xway) {
        this.xway = xway;
    }

    public short getLane() {
        return lane;
    }

    public void setLane(short lane) {
        this.lane = lane;
    }

    public short getDir() {
        return dir;
    }

    public void setDir(short dir) {
        this.dir = dir;
    }

    public short getSeg() {
        return seg;
    }

    public void setSeg(short seg) {
        this.seg = seg;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }
}

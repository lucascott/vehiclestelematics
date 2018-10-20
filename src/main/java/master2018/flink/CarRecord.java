package master2018.flink;

public class CarRecord {
    final int time;
    final String vid;
    final float spd;
    final String xway;
    final short lane;
    final short dir;
    final short seg;
    final int pos;

    public CarRecord(String[] arr) {
        this.time = Integer.parseInt(arr[0]);
        this.vid = arr[1];
        this.spd = Float.parseFloat(arr[2]);
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
}

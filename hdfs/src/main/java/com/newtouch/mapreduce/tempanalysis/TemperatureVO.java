package com.newtouch.mapreduce.tempanalysis;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TemperatureVO implements WritableComparable<TemperatureVO> {
    /**
     * 最低温度
     */
    private int minTemp;
    /**
     * 最高温度
     */
    private int maxTemp;
    /**
     * 平均温度
     */
    private int avgTemp;

    public TemperatureVO() {
    }

    public TemperatureVO(int minTemp, int maxTemp) {
        this.minTemp = minTemp;
        this.maxTemp = maxTemp;
    }

    public int getMinTemp() {
        return minTemp;
    }

    public void setMinTemp(int minTemp) {
        this.minTemp = minTemp;
    }

    public int getMaxTemp() {
        return maxTemp;
    }

    public void setMaxTemp(int maxTemp) {
        this.maxTemp = maxTemp;
    }

    public int getAvgTemp() {
        return avgTemp;
    }

    public void setAvgTemp(int avgTemp) {
        this.avgTemp = avgTemp;
    }

    public int compareTo(TemperatureVO o) {
        return this.getAvgTemp() > o.getAvgTemp() ? 1 : 0;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(minTemp);
        out.writeInt(maxTemp);
        out.writeInt(avgTemp);
    }

    public void readFields(DataInput in) throws IOException {
        minTemp = in.readInt();
        maxTemp = in.readInt();
        avgTemp = in.readInt();
    }

    @Override
    public String toString() {
        return "minTemp=" + minTemp + " maxTemp=" + maxTemp + " avgTemp=" + avgTemp;
    }
}

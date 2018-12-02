package com.newtouch.mapreduce.tempanalysis;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author elfkingw
 */
public class TemperatureVO implements WritableComparable<TemperatureVO> {
    /**
     * 最低温度
     */
    private Double minTemp;
    /**
     * 最高温度
     */
    private Double maxTemp;
    /**
     * 平均温度
     */
    private Double avgTemp;

    public TemperatureVO() {
    }

    public TemperatureVO(Double minTemp, Double maxTemp) {
        this.minTemp = minTemp;
        this.maxTemp = maxTemp;
        this.avgTemp = new Double(0);
    }


    @Override
    public int compareTo(TemperatureVO o) {
        return this.getAvgTemp() > o.getAvgTemp() ? 1 : 0;
    }

    public Double getMinTemp() {
        return minTemp;
    }

    public void setMinTemp(Double minTemp) {
        this.minTemp = minTemp;
    }

    public Double getMaxTemp() {
        return maxTemp;
    }

    public void setMaxTemp(Double maxTemp) {
        this.maxTemp = maxTemp;
    }

    public Double getAvgTemp() {
        return avgTemp;
    }

    public void setAvgTemp(Double avgTemp) {
        this.avgTemp = avgTemp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(minTemp);
        out.writeDouble(maxTemp);
        out.writeDouble(avgTemp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        minTemp = in.readDouble();
        maxTemp = in.readDouble();
        avgTemp = in.readDouble();
    }

    @Override
    public String toString() {
        return "minTemp=" + minTemp + " maxTemp=" + maxTemp + " avgTemp=" + avgTemp;
    }
}

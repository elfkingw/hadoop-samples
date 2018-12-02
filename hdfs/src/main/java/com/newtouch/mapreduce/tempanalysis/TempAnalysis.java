package com.newtouch.mapreduce.tempanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * map和reducer写在一个类里
 *
 * 温度分析mapreduce例子
 * 统计城市历年平度温度、最高温度、最低温度
 * 文件示例：
 * 文件中每一行，每项数据用空格隔开例如：
 *  城市  日期  最低温度 最高温度
 * shenzhen 20170101 12 25
 * @author elfkingw
 */
public class TempAnalysis {
    private final static String HADOOP_ROOT_URL = "hdfs://dev1:9000";

    public static void main(String[] args){
        try {
            Configuration conf = new Configuration();
            //设置输出key 和 value 直接分隔符，分隔符默认为\t
            conf.set("mapred.textoutputformat.ignoreseparator", "true");
            conf.set("mapred.textoutputformat.separator", " ");
            Job job = Job.getInstance(conf);
            job.setJobName("TempAnalysis");
            job.setJarByClass(TempAnalysis.class);
            job.setReducerClass(TempAnalysisReducer.class);
            job.setMapperClass(TempAnalysisMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(TemperatureVO.class);
            Path in = new Path(HADOOP_ROOT_URL + "/home/data/tempanalysis/");
            Path out = new Path(HADOOP_ROOT_URL + "/home/out/tempAnalysis");
            FileInputFormat.setInputPaths(job, in);
            FileOutputFormat.setOutputPath(job, out);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    static class TempAnalysisMapper extends Mapper<LongWritable, Text, Text, TemperatureVO> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String separator = " ";
            int length =2;
            String[] str = line.split(" ");
            if (str.length < length) {
                return;
            }
            String city = str[0];
            Double minTemp = Double.valueOf(str[2]);
            Double maxTemp = Double.valueOf(str[3]);
            TemperatureVO temperatureVO = new TemperatureVO(minTemp, maxTemp);
            context.write(new Text(city), temperatureVO);
        }
    }

    static class TempAnalysisReducer extends Reducer<Text, TemperatureVO, Text, TemperatureVO> {

        @Override
        protected void reduce(Text key, Iterable<TemperatureVO> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Double minTemp = new Double(1000);
            Double maxTemp = new Double(0);
            Double sumTemp = new Double(0);
            Double avgTemp = new Double(0);
            for (TemperatureVO temperatureVO : values) {
                Double minTemperature = temperatureVO.getMinTemp();
                Double maxTemperature = temperatureVO.getMaxTemp();
                if (minTemperature < minTemp) {
                    minTemp = minTemperature;
                }
                if (maxTemperature > maxTemp) {
                    maxTemp = maxTemperature;
                }
                count++;
                sumTemp += (maxTemperature + minTemperature) / 2;
            }
            avgTemp =sumTemp/count;
            TemperatureVO result = new TemperatureVO();
            result.setAvgTemp(avgTemp);
            result.setMaxTemp(maxTemp);
            result.setMinTemp(minTemp);
            context.write(new Text(key),result);
        }
    }
}

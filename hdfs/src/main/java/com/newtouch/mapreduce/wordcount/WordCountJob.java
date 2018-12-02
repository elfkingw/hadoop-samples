package com.newtouch.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author elfkingw
 */
public class WordCountJob {
    private final static String HADOOP_ROOT_URL = "hdfs://dev1:9000";

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJobName("wordCount");
            job.setJarByClass(WordCountJob.class);
            job.setReducerClass(WordReduce.class);
            job.setMapperClass(WordCountMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //设置排序
            job.setSortComparatorClass(IntWritableDecreasingComparator.class);
            Path in = new Path(HADOOP_ROOT_URL + "/home/data/helloworld.txt");
            Path out = new Path(HADOOP_ROOT_URL + "/home/out/helloworld");
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
}

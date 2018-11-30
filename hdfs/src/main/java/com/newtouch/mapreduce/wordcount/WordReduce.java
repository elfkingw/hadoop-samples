package com.newtouch.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *  Reducer<a, b, c, d>
 *      a:mapper输出key
 *      b:mapper输出的value
 *      c:输出文件的内容 key
 *      d:输出文件的内容 value
 */
public class WordReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable c : values) {
            count = count + c.get();
        }
        context.write(key,new IntWritable(count));
    }
}

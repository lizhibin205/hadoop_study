package com.bytrees.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
    	int maxValue = Integer.MIN_VALUE;
    	for (IntWritable val : values) {
    		if (val.get() > maxValue) {
    			maxValue = val.get();
    		}
    	}
    	context.write(key, new IntWritable(maxValue));
    }
}

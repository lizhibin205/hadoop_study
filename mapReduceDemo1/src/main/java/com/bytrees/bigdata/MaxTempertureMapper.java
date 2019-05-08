package com.bytrees.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempertureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
    	String line = value.toString();
    	String yearStr = line.substring(0, 4);
    	String tempertureStr = line.substring(5, 7);
    	try {
    		int temperture = Integer.parseInt(tempertureStr);
    		context.write(new Text(yearStr), new IntWritable(temperture));
    	} catch (NumberFormatException ex) {
    		//do nothing
    	}
    }
}

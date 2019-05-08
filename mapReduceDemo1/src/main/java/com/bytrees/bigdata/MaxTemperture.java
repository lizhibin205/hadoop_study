package com.bytrees.bigdata;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperture {
    public static void main(String[] argv) throws Exception {
    	if (argv.length != 2) {
    		System.err.println("Usage: MaxTemperature <input> <output>");
    		System.exit(-1);
    	}
    	
    	Job job = Job.getInstance();
    	job.setJarByClass(MaxTemperture.class);
    	job.setJobName("Max Tempture");
    	
    	FileInputFormat.addInputPath(job, new Path(argv[0]));
    	FileOutputFormat.setOutputPath(job, new Path(argv[1]));
    	
    	job.setMapperClass(MaxTempertureMapper.class);
    	job.setReducerClass(MaxTempertureReduce.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

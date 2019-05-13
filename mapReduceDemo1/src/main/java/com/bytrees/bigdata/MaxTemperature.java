package com.bytrees.bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperature extends Configured implements Tool {
    public static void main(String[] argv) throws Exception {
    	System.exit(ToolRunner.run(new MaxTemperature(), argv));
    }

    @Override
    public int run(String[] argv) throws Exception {
    	if (argv.length != 2) {
    		System.err.printf("Usage: %s <input> <output>\n", getClass().getName());
    		ToolRunner.printGenericCommandUsage(System.err);
    		return -1;
    	}
    	
    	Job job = Job.getInstance();
    	job.setJarByClass(MaxTemperature.class);
    	job.setJobName("Max Tempture");
    	
    	FileInputFormat.addInputPath(job, new Path(argv[0]));
    	FileOutputFormat.setOutputPath(job, new Path(argv[1]));
    	
    	job.setMapperClass(MaxTemperatureMapper.class);
    	job.setReducerClass(MaxTemperatureReduce.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);

    	return job.waitForCompletion(true) ? 0 : 1;
    }
}

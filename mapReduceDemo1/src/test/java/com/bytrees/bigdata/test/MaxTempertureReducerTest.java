package com.bytrees.bigdata.test;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.bytrees.bigdata.MaxTempertureReduce;

public class MaxTempertureReducerTest {
	@Test
	public void returnMaxTempertureIntegerValues() throws IOException, InterruptedException {
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new MaxTempertureReduce())
        .withInput(new Text("1989"), Arrays.asList(new IntWritable(23), new IntWritable(35), new IntWritable(34)))
        .withOutput(new Text("1989"), new IntWritable(35))
        .runTest();
	}
}

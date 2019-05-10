package com.bytrees.bigdata.test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.bytrees.bigdata.MaxTempertureMapper;


public class MaxTempertureMapperTest {
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text text = new Text("1979 24");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTempertureMapper())
		.withInput(new LongWritable(0), text)
		.withOutput(new Text("1979"), new IntWritable(24))
		.runTest();
	}

	@Test
	public void processesNoRecord()  throws IOException, InterruptedException {
		Text text = new Text("1979 a1");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTempertureMapper())
		.withInput(new LongWritable(0), text)
		.runTest();
	}
}

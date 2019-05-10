package com.bytrees.bigdata;

import java.io.InputStream;
import java.net.URI;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileWriter {
    public static void main(String[] argv) {
        if (argv.length < 1) {
        	System.out.println("Usage: FileWriter [output]");
        	System.exit(-1);
        }

        String openDirectory = argv[0];

    	try {
    		Configuration conf = new Configuration();
    		FileSystem fs = FileSystem.get(URI.create(openDirectory), conf);

    		//读取文件
    		{
	    		InputStream input = fs.open(new Path(openDirectory + "read.txt"));
	    		IOUtils.copy(input, System.out);
	    		input.close();
    		}

    		//写入文件
    		//如果是追加，可以使用append方法
    		{
    			Date date = new Date();
    			FSDataOutputStream fsdOuput = fs.create(new Path(openDirectory + "write.txt"), true);
    			fsdOuput.writeChars("write data use Java api.\n");
    			fsdOuput.writeChars("write at: " + date.toString() + "\n");
    			fsdOuput.close();
    		}

    		System.out.println("bye.");
    	} catch (Exception ex) {
    		ex.printStackTrace(System.err);
    	} finally {
    	}
    }
}

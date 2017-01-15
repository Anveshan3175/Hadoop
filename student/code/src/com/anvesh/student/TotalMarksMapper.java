package com.anvesh.student;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalMarksMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable key,Text line,Context ctx) throws IOException, InterruptedException{
		System.out.println("----------------------------IN the mapper---------------------------");
		String[] tokens = line.toString().split(",");
		if(tokens.length == 3) {
			ctx.write(new Text(tokens[0]), new IntWritable(Integer.parseInt(tokens[2])));
		}
	}
}

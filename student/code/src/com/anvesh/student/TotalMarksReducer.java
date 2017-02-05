package com.anvesh.student;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalMarksReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	public void reduce(Text studentName,Iterable<IntWritable> subMarksItr,Context ctx) throws IOException, InterruptedException{
		System.out.println("----------------------------IN the reducer---------------------------");
		int subMarks = 0,total = 0;
		StringBuilder output = new StringBuilder();
		while(subMarksItr.iterator().hasNext()){
			subMarks = subMarksItr.iterator().next().get();
			output.append(subMarks+",");
			total += subMarks;
		}
		ctx.write(studentName, new Text(output.append("totalMarks :"+total).toString()));
	}
}

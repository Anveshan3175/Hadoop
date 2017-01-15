package com.anvesh.student;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* ip
 * name,subjId,marks
 * 
 * op
 * name,mark1,mark2...,TotalMarks:marks
 * 
 * Command to run
 * hadoop jar /home/cloudera/hadoop/oozie/student/totalMarks.jar com.anvesh.student.StudentMarksDriver /user/cloudera/oozie/student/ip /user/cloudera/oozie/student/op
 * 
 * hadoop fs -cat /user/cloudera/oozie/student/op/part-r-00000
 * 
 * hadoop fs -rm -r /user/cloudera/oozie/student/op/
 * 
 * hadoop fs -copyFromLocal /home/cloudera/hadoop/oozie/student/totalMarks.jar /user/cloudera/oozie/student/lib/
 */
public class StudentMarksDriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		if(args.length == 0 || args.length < 2){
			throw new RuntimeException("Please check the arguments passed in execution");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("This job totals the marks of each student");
		job.setJarByClass(StudentMarksDriver.class);
		
		job.setMapperClass(TotalMarksMapper.class);
		job.setReducerClass(TotalMarksReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the location of input and output file path which are given in the
		// terminal
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Once the job completes, exit from the java execution.
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	} 
	
	/* ip
	 * name,subjId,marks
	 */
	public static class TotalMarksMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		public void map(LongWritable key,Text line,Context ctx) throws IOException, InterruptedException{
			String[] tokens = line.toString().split(",");
			if(tokens.length == 3) {
				ctx.write(new Text(tokens[0]), new IntWritable(Integer.parseInt(tokens[2])));
			}
		}
	}
	
	public static class TotalMarksReducer extends Reducer<Text, IntWritable, Text, Text>{
		
		@Override
		public void reduce(Text studentName,Iterable<IntWritable> subMarksItr,Context ctx) throws IOException, InterruptedException{
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

}

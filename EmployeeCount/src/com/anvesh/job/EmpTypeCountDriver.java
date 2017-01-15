package com.anvesh.job;

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

/* This class counts the different types of employees in the file system provided.
 * 
 * Input to MR will be as in below sample.
 * John,Teacher
 * George,Singer
 * Patrick,Teacher
 * Noor,Engineer
 * Rick,Sailor
 * 
 * Output of the MR will be like in below
 * Teacher,2
 * Singer,1
 * Sailor,1
 * Engineer,1
 * 
 *  Command to run
 * [cloudera@localhost ~]$ hadoop jar /home/cloudera/mapreduce/emptypeCount/empTypeCount.jar com.anvesh.job.EmpTypeCountDriver /user/cloudera/mapreduce/empTypeCount/input/ /user/cloudera/mapreduce/empTypeCount/output/

 */

public class EmpTypeCountDriver {

	/**
	 * @param args
	 * @throws IOException pes o
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub

		if(args == null || args.length < 2){
			throw new RuntimeException("Input/Output file path is missing.Please check");
		}
		
		// Create job instance using config object
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("Count the employees in different categories");
		job.setJarByClass(EmpTypeCountDriver.class);
		
		// output should be  <Text,IntWritable>  (Ex : Engineer,1)
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Static nested mapper class 
		job.setMapperClass(EmpTypeMapper.class);
		
		//Static nested reducer class
		job.setReducerClass(EmpTypeReducer.class);
		 
		//set the location of input and output file path which are given in the terminal  
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		// Once the job completes, exit from the java execution. 
		System.exit(job.waitForCompletion(true) ? 0 : 1 );
		
	}
	
	
	/* input to the mapper (sample)
	 * 1,(John,Teacher)
	 * 2,(Patrick,Teacher)
	 * 3,(Rick,Sailor)
	 * 
	 * output of the mapper
	 * Teacher,1
	 * Teacher,1
	 * Sailor,1
	 * 
	 * We are neglecting the names of the employees in the mapper class. We willl consider only employment types in mapper.
	 */
	public static class EmpTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException, InterruptedException{
			
			String[] tokens = value.toString().split(",");
			
			// we will neglect emp names and take only emp types . Hence token[0] is neglected
			ctx.write(new Text(tokens[1]), new IntWritable(1));
		}
		
	}
	
	/* input to the reducer (sample)
	 * Teacher,1
	 * Teacher,1
	 * Sailor,1
	 * 
	 * output of the reducer (sample)
	 * Teacher,2               -- sum total of all teachers
	 * Sailor,1
	 */
	public static class EmpTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context ctx) throws IOException, InterruptedException{
			
			int sum = 0;
			
			while(values.iterator().hasNext()){
				sum+= values.iterator().next().get();
			}
			
			ctx.write(key,new IntWritable(sum));
		}
	}

}

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


/*
 * The input for this job is :
 * 
 * I am tired of cash crunch scenario. 
 * I am done away with it.
 * Please save me.
 * 
 * 
 * 
 * Reducer output :
 * 1 : [I]
 * 2 : [am,of,it,me]
 * 4 : [cash,done,with,save,away]
 * 5 : [tired]
 * 6 : [crunch,Please]
 * 8 : [scenario]
 * 
 * command to run
 * $ hadoop jar /home/anveshan/hdpl/progs/wordLength/wordLength.jar com.anvesh.job.EqualLengthWordJob /user/hduser/mrprogs/wordLength/ip /user/hduser/mrprogs/wordLength/op/
 */
public class EqualLengthWordJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if(null == args || args.length < 2){
			throw new RuntimeException("Input/Output file path is missing.Please check");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("This program counts the number of words");
		job.setJarByClass(EqualLengthWordJob.class);
		
		// output should be  <Text,IntWritable>  (Ex : Engineer,1)
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Static nested mapper class 
		job.setMapperClass(EqualLengthWordMapper.class);
		
		//Static nested reducer class
		job.setReducerClass(EqualLengthWordReducer.class);
		 
		//set the location of input and output file path which are given in the terminal  
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		// Once the job completes, exit from the java execution. 
		System.exit(job.waitForCompletion(true) ? 0 : 1 );

	}

	public static class EqualLengthWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
			
			//replace all commas before mapping
			String[] tokens = value.toString().replaceAll("," , "").split(" ");
			
			//iterating through all the words available in that line and forming the key value pair
            for (String str : tokens)
            {
               //sending to output collector which inturn passes the same to reducer
               str.toLowerCase();
               ctx.write(new Text(str), new IntWritable(1));
            }
			
		}
	}

	public static class EqualLengthWordReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

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

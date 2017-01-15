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


/* This class counts the words in files provided.
 * 
 * Hello World, Bye World! 
 * Hello World, Welcome World!
 * THis is Hello world program
 * 
 * Output of the MR will be like in below (case sensitive)
 * Hello,3
 * World,3
 * Bye,1
 * World,1
 * world,1
 * Welcome,1
 * THis,1
 * is,1
 * program,1
 * 
 * command to run 
 * $ hadoop jar /home/anveshan/hdpl/progs/wordCount/wordCount.jar com.anvesh.job.WordCountDriver /user/hduser/mrprogs/wordCount/ip /user/hduser/mrprogs/wordCount/op/
 */
public class WordCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		if(null == args || args.length < 2){
			throw new RuntimeException("Input/Output file path is missing.Please check");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("This program counts the number of words");
		job.setJarByClass(WordCountDriver.class);
		
		// output should be  <Text,IntWritable>  (Ex : Engineer,1)
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Set the outputs for the Job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Static nested mapper class 
		job.setMapperClass(WordCountMapper.class);
		
		//Static nested reducer class
		job.setReducerClass(WordCountReducer.class);
		 
		//set the location of input and output file path which are given in the terminal  
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		// Once the job completes, exit from the java execution. 
		System.exit(job.waitForCompletion(true) ? 0 : 1 );
	}
	
	/* Input to the Mapper 
	 * 1,(Hello World, Bye World!)
	 * 2,(Hello World, Welcome World!)
	 * 3,(THis is Hello world program)
	 * 
	 * output of the mapper
	 * Hello,1
	 * World,1
	 * Bye,1
	 * World!,1
	 * Hello,1  .....
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		// Context is inner class in mapreduce.mapper
		@Override
		public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
			
			System.out.println("This is in map code");
			
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
	
	/* Input to reducer
	 * Hello,1
	 * World,1
	 * Bye,1
	 * World!,1
	 * Hello,1  .....
	 * 
	 * Output of the reducer will be 
	 * Hello,3
	 * World,3
	 * Bye,1  ...
	 * 
	 */
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		int sum = 0;
		
		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context ctx) throws IOException, InterruptedException{
			
			System.out.println("This is in reducer reduce code");
			
			while(values.iterator().hasNext()){
				sum+= values.iterator().next().get();
			}
			ctx.write(key,new IntWritable(sum));
		}
		
		@Override
		public void cleanup(Context ctx) throws IOException, InterruptedException{
			ctx.write(new Text("Most repeated word"),new IntWritable(sum));
			System.out.println("This is in reducer cleanup code");
		}
	}

}

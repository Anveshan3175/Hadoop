package com.anvesh.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * hadoop jar /home/anveshan/hdpl/progs/GroupWords/groupWords.jar  com.anvesh.job.GroupWordsDriver /user/hduser/mrprogs/GroupWords/ip /user/hduser/mrprogs/GroupWords/op3/
 * 
 * hadoop fs -cat /user/hduser/mrprogs/GroupWords/op3/part-r-00000
 */

public class GroupWordsDriver {
	public static final Log log = LogFactory.getLog(GroupWordsDriver.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		if(null == args || args.length < 2){
			throw new RuntimeException("Input/Output file path is missing.Please check");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("This program counts the number of words");
		job.setJarByClass(GroupWordsDriver.class);
		
		// Set the outputs for the Map
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Set the outputs for the Job
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Object.class);
		
		//Static nested mapper class 
		job.setMapperClass(GroupWordMapper.class);
		
		//Static nested reducer class
		job.setReducerClass(GroupWordReducer.class);
		 
		//set the location of input and output file path which are given in the terminal  
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		// Once the job completes, exit from the java execution. 
		System.exit(job.waitForCompletion(true) ? 0 : 1 );
	}
	/* Input to the Mapper 
	 *
	 * 
	 * output of the mapper
	 1	I
	 1	I
	 2	am
	 2	am
	 2	me
	 2	it
	 2	of
	 4	with
	 4	away
	 4	done
	 4	save
	 4	cash
	 5	tired
	 6	Please
	 6	crunch
	 8	scenario
	 */
		public static class GroupWordMapper extends Mapper<LongWritable,Text,IntWritable,Text> {	
			// Context is inner class in mapreduce.mapper
			@Override
			public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
				
				//replace all commas before mapping
				String[] tokens = value.toString().replaceAll("," , "").split(" ");
				
				//iterating through all the words available in that line and forming the key value pair
	            for (String str : tokens)
	            {
	               //sending to output collector which in turn passes the same to reducer
	               str.toLowerCase();
	               ctx.write(new IntWritable(str.length()),new Text(str));
	               log.info("--------------------Token is: "+str.toString());
	            }
				
			}
		}
	
	/* 
	 * 
	 1	[I, I]
	 2	[am, am, me, it, of]
	 4	[with, away, done, save, cash]
	 5	[tired]
	 6	[Please, crunch]
	 8	[scenario]
	 * 
	 */
	public static class GroupWordReducer extends Reducer<IntWritable,Text, IntWritable,Object>{
			@Override
			//public void reduce(Text key,Iterable<IntWritable> values,Context ctx) throws IOException, InterruptedException{
			public void reduce(IntWritable key,Iterable<Text> values,Context ctx) throws IOException, InterruptedException{
				List<String> listVals = new ArrayList<String>();
				String val;
				while(values.iterator().hasNext()){
					val= values.iterator().next().toString();
					log.info("---------------------In reducer, the key is "+key +" and value is :"+val);
					listVals.add(val);
				}
				ctx.write(key,listVals);
			}
		}
	
	
	
	
	
	}

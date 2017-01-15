package com.anvesh.job;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/* This class finds the total tracks in the file
 * 
 * i/p
	UserId|TrackId|Shared|Radio|Skip
	111115|222|0|1|0
	111113|225|1|0|0
	111117|223|0|1|1
	111115|225|1|0|0
	111119|226|0|1|1
	111113|225|1|0|0
	111120|227|0|1|1
	111118|226|1|0|0
	111113|225|0|1|0
 * 
 * 
 * o/p
 * No of unique Listeners :  6
 * ..........   
 * 
 * Command to run
 * hadoop jar /home/anveshan/hdpl/progs/trackProjects/uniqListener/uniListener.jar com.anvesh.job.UniqueListenerDriver /user/hduser/mrprogs/trackProjects/uniqListener/ip /user/hduser/mrprogs/trackProjects/uniqListener/op1/
 * 
 * hadoop fs -cat /user/hduser/mrprogs/trackProjects/uniqListener/op1/part-r-00000
 * 
 * hadoop fs -rm -r /user/hduser/mrprogs/trackProjects/uniqListener/op1/
 */


public class UniqueListenerDriver {
	
	public static final Log log = LogFactory.getLog(UniqueListenerDriver.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if(args == null || args.length != 2){
			throw new RuntimeException("Check the hadoop execute command args");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("Doing listener analysis");
		job.setJarByClass(UniqueListenerDriver.class);
		
		job.setMapperClass(UniMapper.class);
		job.setReducerClass(UniReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// set the location of input and output file path which are given in the
		// terminal
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Once the job completes, exit from the java execution.
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	public static class UniMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException, InterruptedException{
			
			log.info("In the UniMapper-------------------------------------");
			String[] ipTokens = value.toString().split("[|]");
			log.info("Inputs"+Arrays.toString(ipTokens));
			if(ipTokens.length == 5 && (!ipTokens[0].equals("UserId"))) {
				ctx.write(new Text(ipTokens[0]), new IntWritable(1));
				log.info("ipTokens[0]"+ipTokens[0]);
			}
		}
	}
	

	public static class UniReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		int uniqueListerner = 0;
		StringBuilder sb = new StringBuilder();
		
		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context ctx) throws IOException, InterruptedException{
			log.info("In the UniReducer------------------------------------------");
			sb.append( key.toString()+" , ");
			log.info("Key is"+key);
			uniqueListerner++;
		}
		
		@Override
		public void cleanup(Context ctx) throws IOException, InterruptedException{
			ctx.write(new Text("The unique Listeners are "+sb.toString()+" and their total is :"), new IntWritable(uniqueListerner));
			System.out.println("This is in reducer cleanup special code");
		}
	}
	

}

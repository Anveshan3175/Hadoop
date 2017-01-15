package com.anvesh.job;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* This class finds the total tracks in the file
 * 
 * i/p
 * 	
   You and Me, Love me life, Mr.International
 * Jhumbalalika,Rangeela,Taal se taal,Humma
 * save me your life
 * 
 * o/p
 * Total tracks : 8
 * ..........   
 * 
 * Command to run
 * hadoop jar /home/anveshan/hdpl/progs/trackProjects/totalTracks/totTracks.jar com.anvesh.job.CountTracksDriver /user/hduser/mrprogs/trackProjects/totalTracks/ip /user/hduser/mrprogs/trackProjects/totalTracks/op1/
 * 
 * hadoop fs -cat /user/hduser/mrprogs/trackProjects/totalTracks/op1/part-r-00000
 */
public class CountTracksDriver {
	
	public static final Log log = LogFactory.getLog(CountTracksDriver.class);
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		if(args == null || args.length < 2){
			throw new RuntimeException("Verify arguments this hadoop program");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJobName("This programs totals the number of the songs in the file");
		
		job.setJarByClass(CountTracksDriver.class);
		
		job.setMapperClass(TrackMapper.class);
		job.setReducerClass(TrackReducer.class);
		
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
	
	public static class TrackMapper extends Mapper<LongWritable, Text,Text,IntWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException{
			String strngs = value.toString();
			StringTokenizer tokens = new StringTokenizer(strngs,",");
			
			while(tokens.hasMoreTokens()){
				//({1,You and Me},{1,Love me life})
				ctx.write(new Text(tokens.nextToken()),new IntWritable(1));
			}
		}
	}
	
	public static class TrackReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		int sumTotal = 0;
		public void reduce(Text key,Iterable<IntWritable> values,Context ctx) throws IOException, InterruptedException{
			String val;
			while(values.iterator().hasNext()){
				sumTotal++;
				val= values.iterator().next().toString();
			}
			System.out.print("--------------value------------ :"+sumTotal);
			//ctx.write(new Text("Total tracks : "), new IntWritable(sumTotal));
		}
		
		@Override
		public void cleanup(Context ctx) throws IOException, InterruptedException{
			ctx.write(new Text("Total tracks : "), new IntWritable(sumTotal));
			System.out.println("This is in reducer cleanup code");
		}
		
	}

}

package com.anvesh.job;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

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

/* This class finds the total tracks of each artist in the file
 * 
 * i/p
 * 	
	Jodha Akhbar, Rehman
	BommaRILLU,Devi
	Shankar dada zindabad,Devi
	AnulaMinnula,Balu
 * 
 * o/p
 * Rehman : 1
 * Devi : 2
 * Balu : 1
 * ..........   
 * 
 * Command to run
 * hadoop jar /home/anveshan/hdpl/progs/trackProjects/artistTracks/artTracks.jar com.anvesh.job.CountArtistSongsDriver /user/hduser/mrprogs/trackProjects/artistTracks/ip /user/hduser/mrprogs/trackProjects/artistTracks/op1/
 * 
 * hadoop fs -cat /user/hduser/mrprogs/trackProjects/artistTracks/op1/part-r-00000
 * 
 * hadoop fs -rm -r /user/hduser/mrprogs/trackProjects/artistTracks/op1
 */

public class CountArtistSongsDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		if(args == null || args.length < 2){
			throw new RuntimeException("Verify arguments this hadoop program");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("Job is set group songs by artist name");
		job.setJarByClass(CountArtistSongsDriver.class);
		
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
	
	
	public static class TrackMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException, InterruptedException{
			String[] tokens = value.toString().split(",");
			if(tokens.length == 2)
				ctx.write(new Text(tokens[1].trim()), new IntWritable(1));
		}
	}
	
	public static class TrackReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		String artistWithHighestSongs ="";
		int highestSongs = 0;
		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context ctx) throws IOException, InterruptedException{
			int songCountForArtist = 0;
			while(values.iterator().hasNext()){
				songCountForArtist += values.iterator().next().get();
			}
			if(songCountForArtist > highestSongs){
				highestSongs = songCountForArtist;
				artistWithHighestSongs = key.toString();
			}
			ctx.write(key, new IntWritable(songCountForArtist));
		}
		
		@Override
		public void cleanup(Context ctx) throws IOException, InterruptedException{
			ctx.write(new Text("Artist With Highest number of Songs is "+artistWithHighestSongs+" with songs :"), new IntWritable(highestSongs));
			System.out.println("This is in reducer cleanup code");
		}
	}

}

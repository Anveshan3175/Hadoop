package com.anvesh.job;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* This class counts the words in files provided.
 * ip1.txt
 * Hello World, Bye World! 
 * 
 * ip2.txt
 * Welcome World, Bye World! 
 * 
 * Output of the MR will be like in below (case sensitive)
 * Hello,ip1,1
 * World,ip1,2
 * World,ip2,2
 * 
 * command to run 
 * $ hadoop jar /home/anveshan/hdpl/progs/wordCount/wordCount.jar com.anvesh.job.WordCountFileDriver /user/hduser/mrprogs/wordCount/ip /user/hduser/mrprogs/wordCount/op/
 * $ hadoop fs -cat /user/hduser/mrprogs/wordCount/op/part-r-00000
 * $ hadoop fs -rm -r /user/hduser/mrprogs/wordCount/op/
 */

public class WordCountFileDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		if (args == null || args.length < 2) {
			throw new RuntimeException("Check the arguments passed");
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJobName("This program counts number of times each word is present in each file");
		job.setJarByClass(WordCountFileDriver.class);

		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// set the location of input and output file path which are given in the
		// terminal
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Once the job completes, exit from the java execution.
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class WordMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text lineText, Context ctx) throws IOException, InterruptedException {
			// Get file name
			InputSplit inputSplit = ctx.getInputSplit();
			String fileName = ((FileSplit) inputSplit).getPath().getName();

			String[] tokens = lineText.toString().split(" ");
			if (tokens != null && tokens.length > 0) {
				for (String word : tokens) {
					word = word.replaceAll("[^a-zA-Z0-9]", "");
					ctx.write((new Text(word)), new Text(fileName));
				}
			}
		}
	}

	public static class WordReducer extends Reducer<Text, Text, Text, IntWritable> {

		int count = 0;
		Map<String, Integer> fileNameMap = new HashMap<>();
		String fileName = null, tempStr = null;
		Iterator<String> finalItr = null;

		@Override
		public void reduce(Text word, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

			while (values.iterator().hasNext()) {
				fileName = values.iterator().next().toString();

				if (null != fileNameMap.get(fileName)) {
					count = fileNameMap.get(fileName).intValue();
					fileNameMap.put(fileName, count++);
				} else {
					fileNameMap.put(fileName, 1);
				}
			}

			finalItr = fileNameMap.keySet().iterator();
			while (finalItr.hasNext()) {
				tempStr = finalItr.next();
				ctx.write(new Text(word +","+tempStr), new IntWritable(fileNameMap.get(tempStr)));
			}

		}
	}

}

package com.anvesh.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * This class groups people having same DNA sequence
 * 
 * The input will be :
 * 
 * user1 ACGT
 * user4 TGC
 * user10 GHC
 * user5 TGC
 * user7 HCM
 * user500 GHC
 * user43 TGC
 * user76 NCD
 * 
 * The output will be :
 * 
 * 	ACGT[user1]
	GHC	[user500, user10]
	HCM	[user7]
	NCD	[user76]
	TGC	[user43, user5, user4]
 * 
 * command to run
 * hadoop jar /home/anveshan/hdpl/progs/GroupDNAs/groupDNA.jar com.anvesh.job.GroupDNAJob /user/hduser/mrprogs/GroupDNAs/ip /user/hduser/mrprogs/GroupDNAs/op/
 * 
 * hadoop fs -cat /user/hduser/mrprogs/GroupDNAs/op/part-r-00000
 */
public class GroupDNAJob {
	public static final Log log = LogFactory.getLog(GroupDNAJob.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		if(null == args || args.length < 2){
			throw new RuntimeException("Input/Output file path is missing.Please check");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("This program groups the users as per DNA sequence");
		
		job.setJarByClass(GroupDNAJob.class);
		
		job.setMapperClass(GroupDNAMapper.class);
		job.setReducerClass(GroupDNAReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Object.class);
		
		//set the location of input and output file path which are given in the terminal  
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				
		// Once the job completes, exit from the java execution. 
		System.exit(job.waitForCompletion(true) ? 0 : 1 );
	}
	
	/*
	 * ip to mapper
	 * 1,(user1 ACGT)
	 * 2,(user4 TGC)
	 * 3,(user10 GHC)
	 * 
	 * op to mapper
	 * ACGT user1
	 * TGC user4
	 * GHC user10
	 * GHC user500
	 */
	public static class GroupDNAMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException, InterruptedException{
			String[] strs = value.toString().split(" ");
			ctx.write(new Text(strs[1]), new Text(strs[0]));
		}
	}
	
	public static class GroupDNAReducer extends Reducer<Text, Text, Text, Object> {
		
		@Override
		public void reduce(Text key,Iterable<Text> values,Context ctx) throws IOException, InterruptedException{
			
			List<String> list = new ArrayList<>();
			String val;
			while(values.iterator().hasNext()){
				val= values.iterator().next().toString();
				log.info("---------------------In reducer, the key is "+key +" and value is :"+val);
				list.add(val);
			}
			ctx.write(key, list);
		}
	}
	
	
	

}

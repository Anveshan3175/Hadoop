package com.anvesh.job;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

/* This class finds the total expenses for the employee
 * 
 * i/p
 * 	empId,dept,category,expenses
	1,sales,travel,100
	2,sales,travel,200
	3,sales,travel,300
	1,sales,salary,4000
	2,sales,salary,3000
	3,sales,salary,2000
	4,Engineering,phone,400
	5,Engineering,phone,500
	6,Engineering,phone,600
	4,Engineering,salary,4000
	5,Engineering,salary,5000
	6,Engineering,salary,6000
	1,Testing,travel,100000
	2,Testing,travel,200000
	3,Testing,travel,300000
	4,Testing,travel,400000
	5,Testing,travel,500000
	6,Testing,travel,600000
 * 
 * o/p
 * 1:sales 4100     
 * 1:Testing 100000
 * 2:sales 3200
 * 2:Testing 200000
 * ..........   
 * 
 * Command to run
 * hadoop jar /home/anveshan/hdpl/progs/empProjects/empSal/empSal.jar com.anvesh.job.EmpSalDeptDriver /user/hduser/mrprogs/empProjects/empSal/ip /user/hduser/mrprogs/empProjects/empSal/op1/
 * 
 * hadoop fs -cat /user/hduser/mrprogs/empProjects/empSal/op1/part-r-00000
 */
public class EmpSalDeptDriver {

	public static final Log log = LogFactory.getLog(EmpSalDeptDriver.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if (args == null || args.length < 2) {
			throw new RuntimeException("Input/Output file path is missing.Please check");
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJobName("This class finds the total expenses for the employee");

		job.setJarByClass(EmpSalDeptDriver.class);

		job.setMapperClass(EmpSalMapper.class);
		job.setReducerClass(EmpSalReducer.class);

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

	public static class EmpSalMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			String[] vals = value.toString().split(",");
			System.out.println("vals "+vals.toString());
			if (!"empId".equals(vals[0])) {
				System.out.println("In mapper : vals[0] "+vals[0].toString() +" , vals[3]"+vals[3].toString());
				ctx.write(new Text(vals[0].toString()+":"+vals[1].toString()),
						new IntWritable(Integer.parseInt(vals[3].toString())));
			}
		}
	}

	public static class EmpSalReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context ctx)
				throws IOException, InterruptedException {
			System.out.println("In reducer");
			int sum = 0,val = 0;
			/*while (values.iterator().hasNext()) {
				val = values.iterator().next().get();
				//System.out.println("IN reducer1 : "+val);
				sum += val;
				System.out.print("value :"+val+", sum :"+sum);
			}*/
			System.out.println();
			ctx.write(key, new IntWritable(sum));
		}
	}

}

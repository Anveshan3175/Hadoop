package com.anvesh.joins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/* This class joins the emp and dept in its simplest form
 * 
 * i/p
	deptId,deptName
	321|Economics
	340|Physics
	405|Mathematics
	307|Astronomy
	
	empId,empName,deptId
	1001|Alan|321
	1002|Camelin|340
	1007|Frank|405
	1005|Tracy|307
	1008|Arun|321
 * 
 * 
 * o/p
 * 1001,Alan,Economics
 * 1002,Camelin,Physics
 * 1007,Frank,Mathematics
 * 1005,Tracy,Astronomy
 * 1008,Arun,Economics
 * 
 * Command to run
 * hadoop jar /home/anveshan/hdpl/progs/joins/Emp_Dept_1/joins.jar com.anvesh.joins.EmpDeptDriver /user/hduser/mrprogs/joins/Emp_Dept_1/emp/ /user/hduser/mrprogs/joins/Emp_Dept_1/dept /user/hduser/mrprogs/joins/Emp_Dept_1/op/ 
 * 
 * hadoop fs -cat /user/hduser/mrprogs/joins/Emp_Dept_1/op/part-r-00000
 * 
 * hadoop fs -rm -r /user/hduser/mrprogs/joins/Emp_Dept_1/op/
 */
public class EmpDeptDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if(args == null || args.length != 3){
			throw new RuntimeException("Check the arguments in the execution command");
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJobName("Emp Dept simple join");
		job.setJarByClass(EmpDeptDriver.class);
		
		//job.setMapperClass(cls);
		job.setReducerClass(EmpDeptReducer.class);
		
		//job.setMapOutputKeyClass(theClass);
		//job.setMapOutputValueClass(theClass);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the location of input and output file path which are given in the
		// terminal
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, EmpMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DeptMapper.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		// Once the job completes, exit from the java execution.
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	/*
	 * empId,empName,deptId
		1001|Alan|321
	 */
	public static class EmpMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException, InterruptedException{
			String[] args = value.toString().split("[|]");
			if(args.length == 3)
				ctx.write(new Text(args[2]), new Text("m_"+args[0]+","+args[1]));
		}
	}
	
	/*
	 * 321|Economics
	 */
	public static class DeptMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException, InterruptedException{
			String[] args = value.toString().split("[|]");
			if(args.length == 2)
				ctx.write(new Text(args[0]), new Text("r_"+args[1]));
		}
	}
	/*
	 * (321,[(m_1001,Alan),(r_Economics),(m_1008,Arun)])
	 * 1001,Alan,Economics
	 * 1008,Arun,Economics
	 */
	public static class EmpDeptReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key,Iterable<Text> values,Context ctx) throws IOException, InterruptedException{
			
			String temp,deptName = null;
			List<String> mapList = new ArrayList<String>();
			while(values.iterator().hasNext()){
				temp = values.iterator().next().toString();
				if(temp.startsWith("m_")){
					mapList.add(temp.replace("m_", ""));
				}
				else if (temp.startsWith("r_")) {
					deptName = temp.replace("r_", "");
				}
			}
			for(String str:mapList){
				ctx.write(new Text(str), new Text(deptName));
			}
		}
		
	}

}

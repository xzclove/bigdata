package com.xzc.mapreduce.test.wordcount;

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

public class WordCountMapReduceOptimize {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf  = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop01-senior.ibeifeng.com:8020");
		
		Job job = Job.getInstance(conf);
		
		job.setJobName("wc");
		job.setJarByClass(WordCountMapReduceOptimize.class);
		
		job.setMapperClass(WordCountMapper.class);
		// 指定Combiner
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/user/hadoop/mapreduce/input/wc2.input"));
		
		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/mapreduce/output3"));
		
		boolean success = job.waitForCompletion(true);
		if(success){
			System.out.println("任务执行成功");
		}
	}
	
	
	
	// 1 map
	// input: key LongWritable value:Text
	// output:key --> Text,value --> IntWritable
	static class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		/**
		 * 重写map方法，实现我们自己的逻辑
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] words = value.toString().split(" ");
			for(String word:words){
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}
	/**
	 * 求和
	 * @author hadoop
	 *
	 */
	static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for(IntWritable i : arg1){
				sum += i.get();
			}
			arg2.write(arg0, new IntWritable(sum));
		}
	}
	
	

}

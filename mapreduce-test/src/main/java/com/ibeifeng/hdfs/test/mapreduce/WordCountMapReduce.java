package com.ibeifeng.hdfs.test.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReuce
 * 
 * @author beifeng
 * 
 */
public class WordCountMapReduce extends Configured implements Tool{

	// step 1: Map Class
	/**
	 * 
	 * public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	 */
	public static class WordCountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text mapOutputKey = new Text();
		private final static IntWritable mapOuputValue = new  IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// line value
			String lineValue = value.toString();
			
			// split
			// String[] strs = lineValue.split(" ");
			StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
			
			// iterator
			while(stringTokenizer.hasMoreTokens()){
				// get word value
				String wordValue = stringTokenizer.nextToken();
				// set value
				mapOutputKey.set(wordValue);;
				// output 
				context.write(mapOutputKey, mapOuputValue);
			}
		}

	}

	// step 2: Reduce Class
	/**
	 * 
	 * public class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
	 */
	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private  IntWritable outputValue = new  IntWritable();
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			// sum tmp
			int sum= 0 ;
			// iterator
			for(IntWritable value: values){
				// total
				sum += value.get();
			}
			// set value
			outputValue.set(sum);
			
			// output
			context.write(key, outputValue);
		}

	}

	
	// step 3: Driver ,component job
	public int run(String[] args) throws Exception {
		// 1: get confifuration
		Configuration configuration = getConf();
		
		// 2: create Job
		Job job = Job.getInstance(configuration, //
				this.getClass().getSimpleName());
		// run jar 
		job.setJarByClass(this.getClass());
		
		// 3: set job 
		// input  -> map  -> reduce -> output
		// 3.1: input
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);
		
		// 3.2: map
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 3.3: reduce
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 3.4: output
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		
		// 4: submit job
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1 ;
		
	}

	// step 4: run program
	public static void main(String[] args) throws Exception {
		// 1: get confifuration
		Configuration configuration = new Configuration();
		
		// int status = new WordCountMapReduce().run(args);
		int status = ToolRunner.run(configuration,//
				new WordCountMapReduce(),//
				args);
		
		System.exit(status);
	}

}

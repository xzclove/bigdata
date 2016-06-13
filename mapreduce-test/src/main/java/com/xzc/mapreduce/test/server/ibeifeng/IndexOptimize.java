package com.xzc.mapreduce.test.server.ibeifeng;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 演示join
 */
public class IndexOptimize extends Configured implements Tool{
	
	private static void printUsage(){
		String usage = "Usage:请指定四个参数 1）Job名称 2）输入文件路径 3）输出文件文件";
		System.err.println(usage);
	}
	
	public static void main(String[] args) throws Exception{
		
		if(args.length != 3){
			printUsage();
			return;
		}
		int exitcode = ToolRunner.run(new IndexOptimize(), args);
		
		if(exitcode == 0){
			System.out.println("任务执行成功");
		}else {
			System.out.println("任务执行失败");
		}
	}
		/**
		 * 对每个文本内容进行分词
		 * 
		 * word  文本标识，次数，位置
		 * 
		 * @author hadoop
		 *
		 */
		static class IndexMapper extends Mapper<Text,Text,Text,Text>{
			
			Text newKey = new Text();
			Text newValue = new Text();
			
			@Override
			protected void map(Text key, Text value, Context context)
					throws IOException, InterruptedException {
				// key : content1 
				// value : 文本内容
				String[] words = value.toString().split(" "); //分词
				Map<String,Integer> counts = new HashMap<String,Integer>();
				Map<String,String> locations = new HashMap<String,String>();
				for(int i=0;i < words.length;i++){
					String word = words[i];
					if(counts.containsKey(word)){
						counts.put(word, counts.get(word) + 1);
					}else{
						counts.put(word, 1);
					}
					
					if(locations.containsKey(word)){
						locations.put(word, locations.get(word)+":"+(i+1));
					}else{
						locations.put(word, i+1 +"");
					}
				}
				
				for(Map.Entry<String, Integer> entry : counts.entrySet()){
					newKey.set(entry.getKey());
					newValue.set(key.toString()+","+entry.getValue()+","+locations.get(entry.getKey()));
					context.write(newKey, newValue);
				}
				
			}
		}
		/**
		 * 进行reduce  Join
		 * @author hadoop
		 *
		 */
		static class IndexReducer extends Reducer<Text,Text,Text,Text>{
			
			Text newValue = new Text();

			@Override
			protected void reduce(Text arg0, Iterable<Text> arg1,
					Context arg2)
					throws IOException, InterruptedException {
				StringBuilder sbuilder = new StringBuilder("");
				int i = 0 ;
				for(Text t:arg1){
					if(i == 0){
						sbuilder.append(t.toString());
					}else{
						sbuilder.append(";"+t.toString());
					}
					i ++ ;
				}
				newValue.set(sbuilder.toString());
				arg2.write(arg0, newValue);
			}
		}
		
		
		static class SelfPartitioner extends HashPartitioner<Text,Text>{
			/**
			 * 返回值 在 0 ～ numReduceTasks-1
			 */
			@Override
			public int getPartition(Text key, Text value, int numReduceTasks) {
				
				// 0 1 2 4
				if(key.toString().startsWith("h")){
					return 0;
				}
				if(key.toString().startsWith("s")){
					return 1;
				}
				
				if(key.toString().startsWith("m")){
					return 2;
				}
				
				return 3;
				
				// 真正在企业中分区逻辑，要先根据数据分布情况
				
				//return super.getPartition(key, value, numReduceTasks);
			}
		}

		@Override
		public int run(String[] args) throws Exception {
			
			Configuration conf  = this.getConf();
			//conf.set("fs.defaultFS", "hdfs://hadoop01-senior.ibeifeng.com:8020");
			
			Job job = Job.getInstance(conf);
			
			job.setJobName(args[0]);
			job.setJarByClass(IndexOptimize.class);
			
			job.setMapperClass(IndexMapper.class);
			job.setCombinerClass(IndexReducer.class);
			job.setReducerClass(IndexReducer.class);
			
			job.setPartitionerClass(SelfPartitioner.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			// 直接将输入内容按照"\t"键分隔开来
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(args[1]));
			
			FileSystem fileSystem  = FileSystem.get(conf);
			Path outdir = new Path(args[2]);
			if(fileSystem.exists(outdir)){
				fileSystem.delete(outdir, true);
			}
			FileOutputFormat.setOutputPath(job, outdir);
			
			job.setNumReduceTasks(4);
			
			boolean success = job.waitForCompletion(true);
			if(success){
				return 0;
			}
			return 1;
		}
}

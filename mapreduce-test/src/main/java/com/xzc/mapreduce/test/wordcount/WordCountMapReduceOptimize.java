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

import com.xzc.mapreduce.test.common.HadoopConfig;
import com.xzc.mapreduce.test.util.HdfsUtil;

/**
 * @desc 统计单词出现次数 优化方法
 * @author 925654140@qq.com
 * @date 创建时间：2016年5月29日 上午9:06:17
 * @version 1.0.0
 */
public class WordCountMapReduceOptimize {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HadoopConfig.HOSTNAME.getContext());
		System.setProperty("HADOOP_USER_NAME", HadoopConfig.USERNAME.getContext());
		Job job = Job.getInstance(conf);

		job.setJobName("wordcount");
		job.setJarByClass(WordCountMapReduceOptimize.class);

		job.setMapperClass(WordCountMapper.class);

		// 指定Combiner, 再将数据传到reduce之前，会先合并一次，优化网络传输
		// 但是 适用与计算可以分割到场景，如果是求平均值到就不适合
		job.setCombinerClass(WordCountReducer.class);

		job.setReducerClass(WordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("/user/hadoop/mapreduce/input/wc.input"));

		HdfsUtil.deleteFile(conf,"/user/hadoop/mapreduce/output8");
		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/mapreduce/output8"));

		boolean success = job.waitForCompletion(true);
		if (success) {
			System.out.println("任务执行成功");
		}
	}

	// 1 map
	// input: key LongWritable value:Text
	// output:key --> Text,value --> IntWritable
	static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		/**
		 * 重写map方法，实现我们自己的逻辑
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] words = value.toString().split(" ");
			for (String word : words) {
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}

	/**
	 * 求和
	 * 
	 * @author hadoop
	 * 
	 */
	static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context arg2) throws IOException,
				InterruptedException {

			int sum = 0;
			for (IntWritable i : arg1) {
				sum += i.get();
			}
			arg2.write(arg0, new IntWritable(sum));
		}
	}

}

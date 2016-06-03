package com.xzc.mapreduce.test.wordcount;

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

import com.xzc.mapreduce.test.common.HadoopConfig;
import com.xzc.mapreduce.test.util.HdfsUtil;

/**
 * @desc 统计单词出现次数 原始方法
 * @author 925654140@qq.com
 * @date 创建时间：2016年5月29日 上午9:06:17
 * @version 1.0.0
 */
public class WordCountMapReduce extends Configured implements Tool {

	/**
	 * step 1: 建立 Map 处理类
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text mapOutputKey = new Text();
		private final static IntWritable mapOuputValue = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 读出到每一行到字符串
			String lineValue = value.toString();

			// 按照 \t\n\r\f 分词
			StringTokenizer stringTokenizer = new StringTokenizer(lineValue);

			// 迭代
			while (stringTokenizer.hasMoreTokens()) {
				String wordValue = stringTokenizer.nextToken();
				// 用全局变量，可以避免重复新建对象
				mapOutputKey.set(wordValue);
				context.write(mapOutputKey, mapOuputValue);
			}
		}
	}

	/**
	 * step 2: 建立处理计算类
	 */
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable outputValue = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			// 存放总量
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			outputValue.set(sum);
			// 通过上下文输出
			context.write(key, outputValue);
		}

	}

	/**
	 * step 32: 建立任务启动类
	 */
	public int run(String[] args) throws Exception {

		// 1: 得到 confifuration
		Configuration conf = getConf();

		// 2: 创建 Job
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());

		// 3、设置执行任务的类
		job.setJarByClass(this.getClass());

		// 4: set job
		// 4.1: input
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);

		// 4.2: map
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// ****************************Shuffle*********************************
		// 1) partitioner
		// job.setPartitionerClass(cls);
		// 2) sort
		// job.setSortComparatorClass(cls);
		// 3) optional,combiner
		// job.setCombinerClass(cls);
		// 4) group
		// job.setGroupingComparatorClass(cls);

		// ****************************Shuffle*********************************

		// 4.3: reduce
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 4.4: output
		Path outPath = new Path(args[1]);
		HdfsUtil.deleteFile(conf,args[1]);
		FileOutputFormat.setOutputPath(job, outPath);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	// step 4: 运行任务
	public static void main(String[] args) throws Exception {
		
		// 1: 得到配置
		Configuration configuration = new Configuration();

		configuration.set("fs.defaultFS", HadoopConfig.HOSTNAME.getContext());
		System.setProperty("HADOOP_USER_NAME",HadoopConfig.USERNAME.getContext());
		
		// set compress
		// configuration.set("mapreduce.map.output.compress", "true");
		// configuration.set("mapreduce.map.output.compress.codec",
		// "org.apache.hadoop.io.compress.SnappyCodec");

		// int status = new WordCountMapReduce().run(args);

		String[] argsPath = { "/user/hadoop/mapreduce/input/wc.input", "/user/hadoop/mapreduce/output5" };
		int status = ToolRunner.run(configuration,//
				new WordCountMapReduce(),//
				argsPath);

		System.exit(status);
	}

}

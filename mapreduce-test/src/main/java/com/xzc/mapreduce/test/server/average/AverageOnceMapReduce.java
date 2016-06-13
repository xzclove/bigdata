package com.xzc.mapreduce.test.server.average;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xzc.mapreduce.test.util.HdfsUtil;

/**
 * @desc 求用户历史消费平均额,消费次数 
 * ----> 记录数据
 * 		 123,2016-09-08,234.67
 *		 123,2016-09-08,454.34 
 *   job1 按照日期求总，job2 把日期对再次求总 求平均值
 * @author 925654140@qq.com
 * @date 创建时间：2016年6月08日 上午9:06:17
 * @version 1.0.0
 */
public class AverageOnceMapReduce extends Configured implements Tool {

	/**
	 * job1 按照日期求总
	 */
	public static class AverageDayMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text mapOutputKey = new Text();

		private Text mapOuputValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 对一行数据进行分隔
			String[] infoArr =value.toString().split(",");
 
			mapOutputKey.set(infoArr[0]+","+infoArr[1]);
			mapOuputValue.set(infoArr[2]);

			context.write(mapOutputKey, mapOuputValue);//123,2016-09-08   234.67
		}
	}

	/**
	 * job1 按照日期求总
	 */
	public static class AverageDayReducer extends Reducer<Text, Text, Text, Text> {

		private Text outputValue = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double sum=0;	
			int count = 0;
			for(Text value:values){
				double money=Double.valueOf(value.toString());
				sum=sum+money;	
				count++;
			}
			// 输出平均数
			outputValue.set(String.valueOf(sum)+","+String.valueOf(count));
			// 通过上下文输出
			context.write(key, outputValue);
		}

	}
	
	/**
	 * job2 按照求总uid
	 */
	public static class AverageOnceMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text mapOutputKey = new Text();

		private Text mapOuputValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		    //  123,2016-09-08	1012.353,3
			// 对一行数据进行分隔
			String[] infoArr =value.toString().split("\t");
			String uid=infoArr[0].substring(0, infoArr[0].indexOf(","));
			mapOutputKey.set(uid);
			mapOuputValue.set(infoArr[1]);

			context.write(mapOutputKey, mapOuputValue);//123    1012.353,3
		}
	}

	/**
	 *  job2 按照求总 uid
	 */
	public static class AverageOnceReducer extends Reducer<Text, Text, Text, Text> {

		private Text outputValue = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double sum=0;
			int count=0;
			try {
				for(Text value:values){
					String[] infoArr =value.toString().split(",");
					double money=Double.valueOf(infoArr[0]);
					sum=sum+money;	
					count=count+Integer.valueOf(infoArr[1]);
				}
			} catch (Exception e) {
				System.out.println("----------");
			}
			System.out.println("test:"+sum+","+count);
			System.out.println(sum/count);
			
			// 输出平均数
			outputValue.set(String.valueOf(sum/count));
			// 通过上下文输出
			context.write(key, outputValue);
		}

	}
	
	@Override
	public int run(String[] args) throws Exception {
		int result = this.runFirstJob(args);
		if (result == 0) {
			System.out.println("第一个任务运行成功");
			int result2 = this.runSecondJob(args);
			return result2;
		}
		return result;
	}

	/**
	 * job1 按照日期求总
	 */
	public int runFirstJob(String[] args) throws Exception {

		// 1: 得到 confifuration
		Configuration conf = HdfsUtil.getConf();

		// 2: 创建 Job
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());

		// 3、设置执行任务的类
		job.setJarByClass(this.getClass());

		// 4: set job
		// 4.1: input
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);

		// 4.2: map
		job.setMapperClass(AverageDayMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		// 4.3: reduce
		job.setReducerClass(AverageDayReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 4.4: output
		Path outPath = new Path(args[1]);
		HdfsUtil.deleteFile(outPath);
		FileOutputFormat.setOutputPath(job, outPath);

		return job.waitForCompletion(true) ? 0 : 1;

	}
	
    private int runSecondJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = HdfsUtil.getConf();

		Job job = Job.getInstance(conf,this.getClass().getSimpleName());

		job.setJarByClass(this.getClass());

		job.setMapperClass(AverageOnceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// 指定Combiner
		job.setReducerClass(AverageOnceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 第二个job以第一个job的输出作为输入
		FileInputFormat.addInputPath(job, new Path(args[1]));

		Path outdir = new Path(args[2]);
		HdfsUtil.deleteFile(outdir);
		FileOutputFormat.setOutputPath(job, outdir);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	// step 4: 运行任务
	public static void main(String[] args) throws Exception {
		// args[0] 是输入路径，args[1] 是输出路径,args[2] 是job2输出路径
		if (args.length != 3) {
			printUsage();
			System.exit(0);
		}

		// 1: 得到配置
		Configuration configuration = HdfsUtil.getConf();


		int status = ToolRunner.run(configuration, new AverageOnceMapReduce(), args);
		if (status == 0) {
			System.out.println("任务执行成功");
		} else {
			System.out.println("任务执行失败");
		}
		System.exit(status);
	}

	private static void printUsage() {
		System.err.println("请输入三个参数，args[0] 是job1输入路径，args[1] 是job1输出路径,args[2] 是job2输出路径!!!");
		//  /user/hadoop/mapreduce/input/average  /user/hadoop/mapreduce/output/average/20160613  /user/hadoop/mapreduce/output/average/20160613once
	}

}

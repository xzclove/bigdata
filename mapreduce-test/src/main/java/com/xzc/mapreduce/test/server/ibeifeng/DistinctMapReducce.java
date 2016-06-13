package com.xzc.mapreduce.test.server.ibeifeng;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * @Des 数据去重 ---> 网站每日访问用户 ---- 用户留存 
 *      输入文件： IP,uid,time,event 
 *      2015-06-05 10:34:25
 *      输出的结果是：time uid1,uid2,uid3
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月3日 下午3:40:24
 * @Version V1.0.0
 */
public class DistinctMapReducce extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage:请指定三个参数 1）Job名称 2）输入文件路径 3）输出文件文件 4）第二个任务的输出路径");
			System.exit(0);
		}
		int exitcode = ToolRunner.run(new DistinctMapReducce(), args);
		if (exitcode == 0) {
			System.out.println("所有任务执行成功！");
		}
	}

	// 1 map
	// uid,time
	// output:key --> Text,value --> NullWritable
	// key： uid+","+ 2016-05-04
	static class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		Text infoKey = new Text();
		NullWritable infoValue = NullWritable.get();

		/**
		 * 重写map方法，实现我们自己的逻辑
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] infos = value.toString().split(",");
			String newKey = infos[0] + "," + infos[1].substring(0, 10);
			infoKey.set(newKey);
			context.write(infoKey, infoValue); // 2699656,2016-05-04
		}
	}

	/**
	 * 去重，对同一key 只输出一条记录
	 */
	static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		NullWritable value = NullWritable.get();

		@Override
		protected void reduce(Text arg0, Iterable<NullWritable> arg1, Context arg2) throws IOException,
				InterruptedException {
			arg2.write(arg0, value);
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

	private int runFirstJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = HdfsUtil.getLocalConf();

		Job job = Job.getInstance(conf);

		job.setJobName(args[0]);
		job.setJarByClass(DistinctMapReducce.class);

		job.setMapperClass(DistinctMapper.class);
		
		// 指定Combiner,减少数据传输量
		job.setCombinerClass(DistinctReducer.class);
		job.setReducerClass(DistinctReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));

		Path outdir = new Path(args[2]);
		HdfsUtil.deleteFile(outdir);
		FileOutputFormat.setOutputPath(job, outdir);

		boolean success = job.waitForCompletion(true);

		if (success) {
			return 0;
		}
		return 1;
	}

	private int runSecondJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = HdfsUtil.getLocalConf();

		Job job = Job.getInstance(conf);

		job.setJobName(args[0]);
		job.setJarByClass(DistinctMapReducce.class);

		job.setMapperClass(DistinctMapper2.class);
		
		// 指定Combiner
		job.setCombinerClass(DistinctReducer2.class);
		job.setReducerClass(DistinctReducer2.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// 第二个job以第一个job的输出作为输入
		FileInputFormat.addInputPath(job, new Path(args[2]));

		Path outdir = new Path(args[3]);
		HdfsUtil.deleteFile(outdir);
		FileOutputFormat.setOutputPath(job, outdir);

		boolean success = job.waitForCompletion(true);

		if (success) {
			return 0;
		}
		return 1;
	}

	// 1 map
	// input: key LongWritable value:Text
	// uid,time  12363625,2016-05-04
	// output: time uid
	//
	static class DistinctMapper2 extends Mapper<LongWritable, Text, Text, Text> {
		
		Text infoKey = new Text();
		Text infoValue = new Text();
		
		/**
		 * 重写map方法，实现我们自己的逻辑
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] infos = value.toString().split(",");

			infoKey.set(infos[1]);
			infoValue.set(infos[0]);
			context.write(infoKey, infoValue); // 2016-05-04,12363625
		}
	}

	/**
	 * 拼接uid
	 * 
	 * @author hadoop
	 *
	 */
	static class DistinctReducer2 extends Reducer<Text, Text, Text, Text> {

		Text resultValue = new Text();

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Context arg2) throws IOException, InterruptedException {
			// 拼接uid，注意不要使用String
			StringBuilder sbuilder = new StringBuilder("");
			int i = 0;
			for (Text t : arg1) {
				if (i == 0) {
					sbuilder.append(t.toString());
				} else {
					sbuilder.append("," + t.toString());
				}
				i++;
			}
			resultValue.set(sbuilder.toString());
			arg2.write(arg0, resultValue);  // 2016-05-04 236633,69696558,55556996
		}
	}

}

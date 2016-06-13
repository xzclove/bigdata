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
 * @desc 求用户历史消费日平均额
 * ----> 记录数据
 * 		 123,2016-09-08,234.67
 *		 123,2016-09-08,454.34 
 * @author 925654140@qq.com
 * @date 创建时间：2016年6月08日 上午9:06:17
 * @version 1.0.0
 */
public class AverageDayMapReduce extends Configured implements Tool {

	/**
	 * step 1: 建立 Map 处理类
	 */
	public static class AverageDayMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text mapOutputKey = new Text();

		// 根据业务场景定制类型
		private Text mapOuputValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			System.out.println(value);
			// 对一行数据进行分隔
			String[] infoArr =value.toString().split(",");
 
			mapOutputKey.set(infoArr[0]+","+infoArr[1]);
			mapOuputValue.set(infoArr[2]);

			context.write(mapOutputKey, mapOuputValue);//123,2016-09-08   234.67
		}
	}

	/**
	 * step 2: 建立处理计算类
	 */
	public static class AverageDayReducer extends Reducer<Text, Text, Text, DoubleWritable> {

		private DoubleWritable outputValue = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double sum=0;
			int count=0;
			for(Text value:values){
				double money=Double.valueOf(value.toString());
				sum=sum+money;
				count++;
			}
			// 输出平均数
			outputValue.set(sum/count);
			// 通过上下文输出
			context.write(key, outputValue);
		}

	}

	/**
	 * step 3: 建立任务启动类
	 */
	public int run(String[] args) throws Exception {

		// 1: 得到 confifuration
		Configuration conf = HdfsUtil.getLocalConf();

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
		job.setOutputValueClass(DoubleWritable.class);

		// 4.4: output
		Path outPath = new Path(args[1]);
		HdfsUtil.deleteFile(outPath);
		FileOutputFormat.setOutputPath(job, outPath);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	// step 4: 运行任务
	public static void main(String[] args) throws Exception {
		// args[0] 是输入路径，args[1] 是输出路径
		if (args.length != 2) {
			printUsage();
			System.exit(0);
		}

		// 1: 得到配置
		Configuration configuration = HdfsUtil.getConf();

		// 测试路径
		// String[] argsPath = { "/user/hadoop/mapreduce/input/wc.input" ,
		// "/user/hadoop/mapreduce/output/" + DateUtil.currentDateHMS() };

		int status = ToolRunner.run(configuration, new AverageDayMapReduce(), args);
		if (status == 0) {
			System.out.println("任务执行成功");
		} else {
			System.out.println("任务执行失败");
		}
		System.exit(status);
	}

	private static void printUsage() {
		System.err.println("请输入两个参数，args[0] 是输入路径，args[1] 是输出路径!!!");
		//  /user/hadoop/mapreduce/input/average  /user/hadoop/mapreduce/output/average/20160613
	}

}

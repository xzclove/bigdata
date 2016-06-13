package com.xzc.mapreduce.test.server.ibeifeng;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xzc.mapreduce.test.util.HdfsUtil;


/**
 * @Des 数据清洗  ---> 得到需要处理的信息
 *  Apache Http 日志
 *  127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)"
 *	ip          uid    time                                                                 url
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月3日 下午3:40:24
 * @Version V1.0.0
 */
public class DataCleanOptimize extends Configured implements Tool {	

	public static void main(String[] args) throws Exception {
		Configuration configuration = HdfsUtil.getLocalConf();
		if (args.length != 3) {
			System.err.println("Usage:请指定三个参数 1）Job名称 2）输入文件路径 3）输出文件文件");
			System.exit(0);
		}
		int exitcode = ToolRunner.run(configuration,new DataCleanOptimize(), args);

		if (exitcode == 0) {
			System.out.println("任务执行成功");
		} else {
			System.out.println("任务执行失败");
		}
	}

	static class DataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		// 正则表达式过滤器
		String patternStr = "^([0-9.]+)\\s([\\w.-]+)\\s" + "([\\w.-]+)\\s(\\[[^\\[\\]]+\\])\\s"
				+ "\"((?:[^\"]|\\\")+)\"\\s" + "(\\d{3})\\s(\\d+|-)\\s" + "\"((?:[^\"]|\\\")+)\"\\s"
				+ "\"((?:[^\"]|\\\")+)\"$";
		Pattern p = Pattern.compile(patternStr);

		Text infoKey = new Text();
		NullWritable infoValue = NullWritable.get();

		/**
		 * 重写map方法，实现我们自己的逻辑
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Matcher matcher = p.matcher(value.toString());

			if (matcher.find()) {
				String ip = matcher.group(1);
				String uid = matcher.group(3);
				String time = matcher.group(4);
				String url = matcher.group(8);
				infoKey.set(ip + "," + uid + "," + time + "," + url);
				context.write(infoKey, infoValue);
			}
		}
	}

	/**
	 * 业务逻辑如果可以做到不用reduce则就不要用reduce，尽量不用reduce
	 * @author hadoop
	 */
    // static class DataCleanReducer extends	 Reducer<Text,NullWritable,Text,Text>{}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = HdfsUtil.getLocalConf();

		Job job = Job.getInstance(conf);

		job.setJobName(args[0]);
		job.setJarByClass(DataCleanOptimize.class);

		job.setMapperClass(DataCleanMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));

		Path outdir = new Path(args[2]);
		HdfsUtil.deleteFile(outdir);
		FileOutputFormat.setOutputPath(job, outdir);
		
		// 这里将reduce的个数设置为零
		job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);
		if (success) {
			return 0;
		}
		return 1;
	}
}

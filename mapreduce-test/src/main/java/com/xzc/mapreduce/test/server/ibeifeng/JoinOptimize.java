package com.xzc.mapreduce.test.server.ibeifeng;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xzc.mapreduce.test.util.HdfsUtil;

/**
 * @Des 表连接  join
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月3日 下午3:40:24
 * @Version V1.0.0
 */
public class JoinOptimize extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.err.println("Usage:请指定三个参数 1）Job名称 2）输入文件路径 3）输出文件文件 4）第二个任务的输出路径");
			System.exit(0);
		}
		int exitcode = ToolRunner.run(new JoinOptimize(), args);

		if (exitcode == 0) {
			System.out.println("任务执行成功");
		} else {
			System.out.println("任务执行失败");
		}
	}

	static class JoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		// 2. MapJoin在内存中通过Map存储小表数据
		Map<Integer, String> acctInosMap = new HashMap<Integer, String>();

		/**
		 * 每个Mapper实例创建出来后，会且只会执行一次该方法
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// 2. 从内存里读取小表文件，使用临时存储
			URI[] files = context.getCacheFiles();
			for (URI u : files) {
				Path p = new Path(u);
				BufferedReader br = null;
				try {
					br = new BufferedReader(new InputStreamReader(new FileInputStream(p.getName())));
					String line = null;
					while ((line = br.readLine()) != null) {
						String[] acctIs = line.split(",");
						acctInosMap.put(Integer.parseInt(acctIs[0]), line.substring(line.indexOf(",") + 1) );
					}
				} finally {
					if (br != null)
						br.close();
				}
			}
			super.setup(context);
		}

		// uid
		IntWritable acctNo = new IntWritable();
		// 该用户ID的
		Text acctValue = new Text();

		/**
		 * 读取大表数据
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 1. Reduce Join获取正在处理的keyvalue对数据来源文件
			FileSplit fs = (FileSplit) context.getInputSplit();
			String fileName = fs.getPath().getName();

			String[] infos = value.toString().split(",");

			acctNo.set(Integer.parseInt(infos[0]));
			
			// 先判断acctNo在小表数据里面存在不存在
			String acctBaseInfo = acctInosMap.get(Integer.parseInt(infos[0]));
			if (acctBaseInfo == null) {
				return;
			}
			String oldValue = value.toString().substring(value.toString().indexOf(",") + 1);
			String newValue = new String();
			// 2. Reduce Join添加标记
			if ("maxConsum.input".equals(fileName)) {
				// 给value值添加标记 0
				newValue = "0," + oldValue;
			} else if ("minConsum.input".equals(fileName)) {
				// 给value值添加标记 1
				newValue = "1," + oldValue;

			} else if ("avgConsum.input".equals(fileName)) {
				// 给value值添加标记 2
				newValue = "2," + oldValue;

			}
			// 3. Map Join 拼接小表的数据
			newValue = acctBaseInfo + ":" + newValue;
			acctValue.set(newValue);
			context.write(acctNo, acctValue);
		}
	}

	/**
	 * 进行reduce Join
	 */
	static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		Text newValue = new Text();

		@Override
		protected void reduce(IntWritable arg0, Iterable<Text> arg1, Context arg2) throws IOException,
				InterruptedException {
			// 3. Reduce Join 先构造集合盛装不同标记的value值
			List<String> max = new ArrayList<String>();
			List<String> min = new ArrayList<String>();
			List<String> avg = new ArrayList<String>();
			// 4.Reduce join 根据不同的标记分选数据
			String acctInfo = new String();
			for (Text t : arg1) {
				String[] acctInfos = t.toString().split(":");
				acctInfo = acctInfos[0];
				String consumInfo = acctInfos[1];
				if (consumInfo.startsWith("0")) {
					max.add(consumInfo.substring(2));
				} else if (consumInfo.startsWith("1")) {
					min.add(consumInfo.substring(2));
				} else if (consumInfo.startsWith("2")) {
					avg.add(consumInfo.substring(2));
				}
			}

			// 5 Reduce Join 迪卡尔积(集合的嵌套遍历)
			for (String s1 : max) {
				for (String s2 : min) {
					for (String s3 : avg) {
						newValue.set(acctInfo + "," + s1 + "," + s2 + "," + s3);
						arg2.write(arg0, newValue);
					}
				}
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = HdfsUtil.getLocalConf();

		Job job = Job.getInstance(conf);

		job.setJobName(args[0]);
		job.setJarByClass(JoinOptimize.class);

		// 1.MapJoin 将指定文件加载到内存
		job.addCacheFile(new Path(args[1]).toUri());

		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

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
}

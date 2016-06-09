package com.xzc.mapreduce.test.server.amtsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xzc.mapreduce.test.util.HdfsUtil;
/**
 * @desc  帐号,消费金额  进行二次排序，先按照帐号升序，帐号相同，按照消费金额降序
 * @author 925654140@qq.com
 * @date 创建时间：2016年5月29日 上午9:06:17
 * @version 1.0.0
 */
public class AmtSortOptimize extends Configured implements Tool {


	enum Counters {
		biggerThan50;
	}

	private static void printUsage() {
		String usage = "Usage:请指定三个参数 1）Job名称 2）输入文件路径 3）输出文件文件";
		System.err.println(usage);
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			printUsage();
			return;
		}
		int exitcode = ToolRunner.run(new AmtSortOptimize(), args);

		if (exitcode == 0) {
			System.out.println("任务执行成功");
		} else {
			System.out.println("任务执行失败");
		}
	}

	/**
	 * 自定义key值
	 * 
	 * @author hadoop
	 * 
	 */
	static class MyKey implements WritableComparable<MyKey> {

		private int acctNo;
		private double amt;

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(acctNo);
			out.writeDouble(amt);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.acctNo = in.readInt();
			this.amt = in.readDouble();
		}

		/**
		 * 比较两个Key值是否相同
		 */
		@Override
		public int compareTo(MyKey other) {

			int result = Integer.compare(this.acctNo, other.getAcctNo());
			if (result == 0) {
				return Double.compare(this.amt, other.getAmt());
			}
			return result;
		}

		public int getAcctNo() {
			return acctNo;
		}

		public void setAcctNo(int acctNo) {
			this.acctNo = acctNo;
		}

		public double getAmt() {
			return amt;
		}

		public void setAmt(double amt) {
			this.amt = amt;
		}

	}

	static class AmtSortMapper extends Mapper<LongWritable, Text, MyKey, NullWritable> {
		/**
		 * 重写map方法，实现我们自己的逻辑
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] amtAcct = value.toString().split(",");
			int acctNo = 0;
			double amt = 0;
			// 捕获数据转换异常
			try {
				acctNo = Integer.parseInt(amtAcct[0]);
				amt = Double.parseDouble(amtAcct[1]);
			} catch (Exception e) {
				return;
			}

			if (amt > 50) {
				context.getCounter(Counters.biggerThan50).increment(1);
			}
			MyKey myKey = new MyKey();
			myKey.setAcctNo(acctNo);
			myKey.setAmt(amt);
			context.write(myKey, NullWritable.get());
		}
	}

	/**
	 * 
	 * @author hadoop
	 * 
	 */
	static class AmtSortReducer extends Reducer<MyKey, NullWritable, IntWritable, DoubleWritable> {

		@Override
		protected void reduce(MyKey myKey, Iterable<NullWritable> arg1, Context context) throws IOException,
				InterruptedException {

			context.write(new IntWritable(myKey.getAcctNo()), new DoubleWritable(myKey.getAmt()));
		}
	}

	/**
	 * 自定义排序
	 * 
	 * @author hadoop
	 * 
	 */
	static class MySelfSorter extends WritableComparator {

		public MySelfSorter() {
			// 告诉这个排序类，key类型为MyKey，第二个参数一定要为true
			super(MyKey.class, true);
		}

		/**
		 * 重写该方法，实现我们自己的排序逻辑
		 */
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// 类型转换
			MyKey myKeyA = (MyKey) a;
			MyKey myKeyB = (MyKey) b;

			// 按帐号正序排列，按金额倒序排列
			int result = Integer.compare(myKeyA.getAcctNo(), myKeyB.getAcctNo());
			if (result == 0) {
				int result2 = -Double.compare(myKeyA.getAmt(), myKeyB.getAmt());
				return result2;
			}
			return result;
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = HdfsUtil.getConf();
		Job job = Job.getInstance(conf);

		job.setJobName(args[0]);
		job.setJarByClass(AmtSortOptimize.class);

		job.setMapperClass(AmtSortMapper.class);

		// 指定排序的类为自定义的排序类
		job.setSortComparatorClass(MySelfSorter.class);
		job.setReducerClass(AmtSortReducer.class);

		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));

		Path outdir = new Path(args[2]);

		HdfsUtil.deleteFile(args[2]);
		FileOutputFormat.setOutputPath(job, outdir);

		boolean success = job.waitForCompletion(true);
		if (success) {
			return 0;
		}
		return 1;
	}
}

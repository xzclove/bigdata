package com.xzc.mapreduce.test.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.xzc.mapreduce.test.util.DateUtil;
import com.xzc.mapreduce.test.util.HdfsUtil;

/**
 * @desc 倒排索引 运行类
 * @author xzc
 *
 */
public class ReverseIndexRunner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = HdfsUtil.getConf();
		Job job = Job.getInstance(conf, "reverse_index");

		FileInputFormat.setInputPaths(job, "/user/hadoop/mapreduce/input");
		job.setJarByClass(ReverseIndexRunner.class);
		job.setMapperClass(ReverseIndexMapper.class);
		job.setReducerClass(ReverseIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/mapreduce/index/" + DateUtil.currentDateHMS()));
		job.waitForCompletion(true);
	}
}

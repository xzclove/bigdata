package com.xzc.mapreduce.test.index;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @desc 倒排索引 map处理类
 * @author gerry
 *
 */
public class ReverseIndexMapper extends Mapper<Object, Text, Text, Text> {
	private Text word = new Text();
	private Text ovalue = new Text();
	private String filePath;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		FileSplit split = (FileSplit) context.getInputSplit();
		filePath = split.getPath().toString();
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			ovalue.set(filePath + ":1");
			context.write(word, ovalue);
		}
	}
}

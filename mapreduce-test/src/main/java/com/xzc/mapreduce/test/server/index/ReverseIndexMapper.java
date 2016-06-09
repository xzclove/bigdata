package com.xzc.mapreduce.test.server.index;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @desc 倒排索引 map处理类
 * @author xzc
 *
 */
public class ReverseIndexMapper extends Mapper<Object, Text, Text, Text> {
	
	private Text word = new Text();
	private Text ovalue = new Text();
	private String filePath;

	/**
	 * 初始化方法
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		// 得到文档信息
		FileSplit split = (FileSplit) context.getInputSplit();
		filePath = split.getPath().toString();
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		// 字符串分割类
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			//  hadoop : doc1:1 
			ovalue.set(filePath + ":1");
			context.write(word, ovalue);
		}
	}
}

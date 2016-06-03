package com.xzc.mapreduce.test.index;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @desc 倒排索引 Reducer处理类
 * @author xzc
 *
 */
public class ReverseIndexReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text outputValue = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		StringBuffer sb = new StringBuffer();
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		
		//  hadoop : doc1:1
		//  hadoop : doc1:1
		//  hadoop : doc2:1	 
		//  value  ==  doc1:1
		for (Text value : values) {
			sb = new StringBuffer();
			
			// line ==  doc1:1
			String line = value.toString(); 
			
			// 将原字符串倒转，避免文档路径本身包含: 导致截取失败
			line = sb.append(line).reverse().toString();
			
			String[] strs = line.split(":", 2);
			String path = sb.delete(0, sb.length() - 1).append(strs[1]).reverse().toString();
			int count = Integer.valueOf(strs[0]);
			if (map.containsKey(path)) {
				// 如果文档相同，则累加
				map.put(path, map.get(path) + count);
			} else {
				// 如果之前没有，则重新加
				map.put(path, count);
			}
		}

		sb = new StringBuffer();
		for (Map.Entry<String, Integer> entry : map.entrySet()) {
			//  结构类似   doc1:2;doc2:1
			sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
		}

		//  去掉最后一个分号
		outputValue.set(sb.deleteCharAt(sb.length() - 1).toString());
		context.write(key, outputValue);
	}
}

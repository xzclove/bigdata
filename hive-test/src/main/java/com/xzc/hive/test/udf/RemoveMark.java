package com.xzc.hive.test.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Desc 去掉 字符中的 "
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月24日 下午5:16:51
 * @Version V1.0.0
 */
public class RemoveMark extends UDF {

	public static Text evaluate(Text str) {

		if (null == str) {
			return null;
		}
		return new Text(str.toString().replaceAll("\"", ""));
	}

	public static void main(String[] args) {
		Text str = new Text("\"123456\"");
		System.out.println(evaluate(str));
	}
}
package com.xzc.hive.test.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * @desc 大小写转换
 * @author 925654140@qq.com
 * @date 创建时间：2016年5月29日 上午9:06:17
 * @version 1.0.0
 */
public class UDFLowerOrUpperCase extends UDF {
	/**
	 * 转换小写
	 * 
	 * @param t
	 * @return
	 */
	public Text evaluate(Text t) {
		// 默认进行小写转换
		return this.evaluate(t, "lower");
	}

	/**
	 * 对参数t进行大小写转换
	 * 
	 * @param t
	 * @param lowerOrUpper
	 *            如果该值为lower，则进行小写转换，如果该值为upper则进行大写转换，其他情况不进行转换。
	 * @return
	 */
	public Text evaluate(Text t, String lowerOrUpper) {
		if (t == null) {
			return t;
		}
		if ("lower".equals(lowerOrUpper)) {
			return new Text(t.toString().toLowerCase());
		} else if ("upper".equals(lowerOrUpper)) {
			return new Text(t.toString().toUpperCase());
		}
		// 转换参数错误的情况下，直接返回原本的值
		return t;
	}
}

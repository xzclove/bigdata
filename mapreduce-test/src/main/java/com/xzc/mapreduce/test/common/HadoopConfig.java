package com.xzc.mapreduce.test.common;

/**
 * @desc 统计单词出现次数 原始方法
 * @author 925654140@qq.com
 * @date 创建时间：2016年5月29日 上午9:06:17
 * @version 1.0.0
 */

public enum HadoopConfig {

	HOSTNAME("hdfs://localhost.hadoop1:9000/"), USERNAME("hadoop");

	public String context;

	public String getContext() {
		return this.context;
	}

	private HadoopConfig(String context) {
		this.context = context;
	}

	public static void main(String[] args) {
		System.out.println(HadoopConfig.HOSTNAME.getContext());
		System.out.println(HadoopConfig.USERNAME.getContext());
	}
}

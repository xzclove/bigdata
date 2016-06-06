package com.xzc.hbase.test.common;

import com.xzc.hbase.test.util.ConfigHolder;

/**
 * @desc  配置参数枚举类
 * @author 925654140@qq.com
 * @date 创建时间：2016年5月29日 上午9:06:17
 * @version 1.0.0
 */

public enum HadoopConfig {

	HOSTNAME(ConfigHolder.getConfig("hbase.hostname", "localhost.hadoop"));

	public String context;

	public String getContext() {
		return this.context;
	}

	private HadoopConfig(String context) {
		this.context = context;
	}

	public static void main(String[] args) {
		System.out.println(HadoopConfig.HOSTNAME.getContext());
	}
}

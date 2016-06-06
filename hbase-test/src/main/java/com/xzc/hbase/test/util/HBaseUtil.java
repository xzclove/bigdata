package com.xzc.hbase.test.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @Des 获取hbase的配置文件信息
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月6日 下午7:11:27
 * @Version V1.0.0
 */
public class HBaseUtil {
	public static Configuration getHBaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.189.100");
		return conf;
	}
}

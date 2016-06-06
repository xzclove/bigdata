package com.xzc.hbase.test.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.xzc.hbase.test.common.HadoopConfig;

/**
 * @Des 获取hbase的配置文件信息
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月6日 下午7:11:27
 * @Version V1.0.0
 */
public class HBaseUtil {
	public static Configuration getHBaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", HadoopConfig.HOSTNAME.getContext());
		return conf;
	}
}

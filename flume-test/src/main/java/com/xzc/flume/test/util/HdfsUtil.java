package com.xzc.flume.test.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.xzc.flume.test.common.HadoopConfig;

/**
 * @Des HDFS 工具类
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月3日 下午1:36:18
 * @Version V1.0.0
 */
public class HdfsUtil {

	public static FileSystem getFS() {
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(getConf());
			return fileSystem;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static Configuration getConf() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HadoopConfig.HOSTNAME.getContext());
		System.setProperty("HADOOP_USER_NAME", HadoopConfig.USERNAME.getContext());
		return conf;
	}

	public static boolean deleteFile(String path) throws IOException {
		FileSystem fs = getFS();
		try {
			// 如果存在就删掉
			if (fs.exists(new Path(path))) {
				System.out.println("####  exist " + path);
				return fs.delete(new Path(path), true);
			}
			return false;
		} finally {
			fs.close();
		}
	}

}

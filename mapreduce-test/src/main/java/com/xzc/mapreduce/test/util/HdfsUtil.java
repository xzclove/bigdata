package com.xzc.mapreduce.test.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @Des HDFS 工具类
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月3日 下午1:36:18
 * @Version V1.0.0
 */
public class HdfsUtil {
	
	public static boolean deleteFile(Configuration conf, String path) throws IOException {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			// 如果存在就删掉
			if (fs.exists(new Path(path))) {
				System.out.println("####  exist "+path);
				return fs.delete(new Path(path), true);
			}
			return false;
		} finally {
			fs.close();
		}
	}
	
}

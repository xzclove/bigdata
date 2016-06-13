package com.xzc.demo.test.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.xzc.demo.test.common.HadoopConfig;

/**
 * @Des HDFS 工具类
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月3日 下午1:36:18
 * @Version V1.0.0
 */
public class HdfsUtil {

	/**
	 * 配置中心
	 */
	private static Configuration conf = new Configuration();

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

	/**
	 * HDFS 模式的配置中心
	 */
	public static Configuration getConf() {
		conf.set("fs.defaultFS", HadoopConfig.HOSTNAME.getContext());
		System.setProperty("HADOOP_USER_NAME", HadoopConfig.USERNAME.getContext());

		// set compress
		// configuration.set("mapreduce.map.output.compress", "true");
		// configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

		return conf;
	}

	/**
	 * 本地模式的配置中心
	 */
	public static Configuration getLocalConf() {
		return conf;
	}

	/**
	 *   如果HDFS存在就删除文件
	 */
	/**
	 *   如果HDFS存在就删除文件
	 */
	public static boolean deleteFile(Path outdir) throws IOException {
		FileSystem fs = getFS();
		try {
			// 如果存在就删掉
			if (fs.exists(outdir)) {
				System.out.println("####  exist " + outdir.getName());
				return fs.delete(outdir, true);
			}
			return false;
		} finally {
			fs.close();
		}
	}	

}

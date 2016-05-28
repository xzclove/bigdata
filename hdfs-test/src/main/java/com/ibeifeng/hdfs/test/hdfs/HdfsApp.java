package com.ibeifeng.hdfs.test.hdfs;

import java.io.File;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 
 * @author beifeng
 * 
 */
public class HdfsApp {

	/**
	 * Get FileSystem
	 * 
	 * @return
	 * @throws Exception
	 */
	public static FileSystem getFileSystem() throws Exception {
		// core-site.xml,core-defautl.xml,hdfs-site.xml,hdfs-default.xml
		Configuration conf = new Configuration();

		// get filesystem
		FileSystem fileSystem = FileSystem.get(conf);

		// System.out.println(fileSystem);
		return fileSystem;
	}

	/**
	 * Read Data
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	public static void read(String fileName) throws Exception {

		// get filesystem
		FileSystem fileSystem = getFileSystem();

		// String fileName = "/user/beifeng/mapreduce/wordcount/input/wc.input";
		// read path
		Path readPath = new Path(fileName);

		// open file
		FSDataInputStream inStream = fileSystem.open(readPath);

		try {
			// read
			IOUtils.copyBytes(inStream, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// close Stream
			IOUtils.closeStream(inStream);
		}
	}

	public static void main(String[] args) throws Exception {

		// String fileName = "/user/beifeng/mapreduce/wordcount/input/wc.input";
		// read(fileName);

		// get filesystem
		FileSystem fileSystem = getFileSystem();

		// write path
		String putFileName = "/user/beifeng/put-wc.inut";
		Path writePath = new Path(putFileName);

		// Output Stream
		FSDataOutputStream outStream = fileSystem.create(writePath);

		// file input Stream
		FileInputStream inStream = new FileInputStream(//
				new File("/opt/modules/hadoop-2.5.0/wc.input")//
		);

		// stream read/write
		try {
			// read
			IOUtils.copyBytes(inStream, outStream, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// close Stream
			IOUtils.closeStream(inStream);
			IOUtils.closeStream(outStream);
		}

	}

}

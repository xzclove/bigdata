package com.xzc.hdfs.test.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.xzc.hdfs.test.util.HdfsUtil;

/**
 * @desc HDFS API 基本测试类
 * @author 925654140@qq.com
 * @date 创建时间：2016年5月29日 上午9:06:17
 * @version 1.0.0
 */
public class CloudDisk {


	public static void main(String[] args) {
		test();
	}

	public static void test() {
		System.out.println("请输入你想进行的操作：");
		System.out.println("\t1、ls --查看目录下的文件和子目录\t");
		System.out.println("\t2、cat --查看文件内容\t");
		System.out.println("\t3、mkdir --创建目录\t");
		System.out.println("\t4、rm --删除文件或者空目录\t");
		System.out.println("\t5、rmr --删除文件或者目录\t");
		System.out.println("\t6、put --上传文件到HDFS\t");
		System.out.println("\t7、get --将HDFS上的文件下载到本地\n");

		Scanner scanner = new Scanner(System.in);
		CloudDisk cd = new CloudDisk();

		String operation = scanner.nextLine();
		if (Integer.valueOf(operation) == 1) {
			System.out.println("请指定文件或目录的路径：");
			String path = scanner.nextLine();
			cd.listFiles(path);
			test();
		} else if (Integer.valueOf(operation) == 2) {
			System.out.println("请指定要查看内容的文件：");
			String file = scanner.nextLine();
			cd.cat(file);
			test();
		} else if (Integer.valueOf(operation) == 3) {
			System.out.println("Create directory or file , give me the path:");
			String file = scanner.nextLine();
			cd.mkdir(file);
			test();
		} else if (Integer.valueOf(operation) == 4) {
			String file = scanner.nextLine();
			cd.rm(file, false);
			test();
		} else if (Integer.valueOf(operation) == 5) {
			String file = scanner.nextLine();
			cd.rm(file, true);
			test();
		} else if (Integer.valueOf(operation) == 6) {
			cd.put("/home/hadoop/wc.input", "/user/hadoop/mapreduce/input/wc2.input");
			test();
		} else if (Integer.valueOf(operation) == 7) {
			cd.get("/home/hadoop/wc3.input", "/user/hadoop/mapreduce/input/wc2.input");
			test();
		} else {
			scanner.close();
			throw new RuntimeException("未知的操作指令");
		}

		scanner.close();
	}


	/**
	 * 查看文件
	 */
	public void listFiles(String specialPath) {
		FileSystem fileSystem = HdfsUtil.getFS();
		try {
			FileStatus[] fstats = fileSystem.listStatus(new Path(specialPath));

			for (FileStatus fstat : fstats) {
				System.out.println(fstat.isDirectory() ? "directory" : "file");
				System.out.println("Permission:" + fstat.getPermission());
				System.out.println("Owner:" + fstat.getOwner());
				System.out.println("Group:" + fstat.getGroup());
				System.out.println("Size:" + fstat.getLen());
				System.out.println("Replication:" + fstat.getReplication());
				System.out.println("Block Size:" + fstat.getBlockSize());
				System.out.println("Name:" + fstat.getPath());

				System.out.println("#############################");
			}

		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("link err");
		} finally {
			if (fileSystem != null) {
				try {
					fileSystem.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	/**
	 * cat
	 * 
	 * @param hdfsFilePath
	 */
	public void cat(String hdfsFilePath) {
		FileSystem fileSystem = HdfsUtil.getFS();
		try {

			FSDataInputStream fdis = fileSystem.open(new Path(hdfsFilePath));

			IOUtils.copyBytes(fdis, System.out, 1024);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fileSystem);
		}

	}

	/**
	 * 创建目录
	 * 
	 * @param hdfsFilePath
	 */
	public void mkdir(String hdfsFilePath) {

		FileSystem fileSystem = HdfsUtil.getFS();

		try {
			boolean success = fileSystem.mkdirs(new Path(hdfsFilePath));
			if (success) {
				System.out.println("Create directory or file successfully");
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			this.closeFS(fileSystem);
		}

	}

	/**
	 * 删除文件或目录
	 * 
	 * @param hdfsFilePath
	 * @param recursive
	 */
	public void rm(String hdfsFilePath, boolean recursive) {
		FileSystem fileSystem = HdfsUtil.getFS();
		try {
			boolean success = fileSystem.delete(new Path(hdfsFilePath), recursive);
			if (success) {
				System.out.println("delete successfully");
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			this.closeFS(fileSystem);
		}
	}

	/**
	 * 上传文件到HDFS
	 * 
	 * @param localFilePath
	 * @param hdfsFilePath
	 */
	public void put(String localFilePath, String hdfsFilePath) {
		FileSystem fileSystem = HdfsUtil.getFS();
		try {
			FSDataOutputStream fdos = fileSystem.create(new Path(hdfsFilePath));
			FileInputStream fis = new FileInputStream(new File(localFilePath));
			IOUtils.copyBytes(fis, fdos, 1024);

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fileSystem);
		}
	}

	/**
	 * 下载文件到本地
	 * 
	 * @param localFilePath
	 * @param hdfsFilePath
	 */
	public void get(String localFilePath, String hdfsFilePath) {
		FileSystem fileSystem = HdfsUtil.getFS();
		try {
			FSDataInputStream fsis = fileSystem.open(new Path(hdfsFilePath));
			FileOutputStream fos = new FileOutputStream(new File(localFilePath));
			IOUtils.copyBytes(fsis, fos, 1024);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(fileSystem);
		}
	}

	/**
	 * 关闭FileSystem
	 * 
	 * @param fileSystem
	 */
	private void closeFS(FileSystem fileSystem) {
		if (fileSystem != null) {
			try {
				fileSystem.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}

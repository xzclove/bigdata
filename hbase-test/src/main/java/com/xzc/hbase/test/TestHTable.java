package com.xzc.hbase.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.xzc.hbase.test.util.HBaseUtil;

/**
 * @Des 类描叙
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月6日 下午7:11:07
 * @Version V1.0.0
 */
public class TestHTable {
	static byte[] family = Bytes.toBytes("f");

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseUtil.getHBaseConfiguration();
		testUseHbaseConnectionPool(conf);
		// testUseHTable(conf);

	}

	static void testUseHTable(Configuration conf) throws IOException {
		HTable hTable = new HTable(conf, "users");
		try {
			// testPut(hTable);
			// testGet(hTable);
			// testDelete(hTable);
			testScan(hTable);
		} finally {
			hTable.close();
		}
	}

	static void testUseHbaseConnectionPool(Configuration conf) throws IOException {
		ExecutorService threads = Executors.newFixedThreadPool(10);
		HConnection pool = HConnectionManager.createConnection(conf, threads);
		HTableInterface hTable = pool.getTable("users");
		try {
			// testPut(hTable);
			// testGet(hTable);
			// testDelete(hTable);
			testScan(hTable);
		} finally {
			hTable.close(); // 每次htable操作完 关闭 其实是放到pool中
			pool.close(); // 最终的时候关闭
		}
	}

	/**
	 * 测试scan
	 * 
	 * @param hTable
	 * @throws IOException
	 */
	static void testScan(HTableInterface hTable) throws IOException {
		Scan scan = new Scan();
		// 增加起始row key
		scan.setStartRow(Bytes.toBytes("row1"));
		scan.setStopRow(Bytes.toBytes("row5"));
		// 增加过滤filter
		FilterList list = new FilterList(Operator.MUST_PASS_ALL);
		byte[][] prefixes = new byte[2][];
		prefixes[0] = Bytes.toBytes("id");
		prefixes[1] = Bytes.toBytes("name");
		MultipleColumnPrefixFilter mcpf = new MultipleColumnPrefixFilter(prefixes);
		list.addFilter(mcpf);
		scan.setFilter(list);

		ResultScanner rs = hTable.getScanner(scan);
		Iterator<Result> iter = rs.iterator();
		while (iter.hasNext()) {
			Result result = iter.next();
			printResult(result);
		}
	}

	/**
	 * 打印result对象
	 * 
	 * @param result
	 */
	static void printResult(Result result) {
		System.out.println("*********************" + Bytes.toString(result.getRow()));
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
		for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
			String family = Bytes.toString(entry.getKey());
			for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : entry.getValue().entrySet()) {
				String column = Bytes.toString(columnEntry.getKey());
				String value = "";
				if ("age".equals(column)) {
					value = "" + Bytes.toInt(columnEntry.getValue().firstEntry().getValue());
				} else {
					value = Bytes.toString(columnEntry.getValue().firstEntry().getValue());
				}
				System.out.println(family + ":" + column + ":" + value);
			}
		}
	}

	/**
	 * 测试put操作
	 * 
	 * @param hTable
	 * @throws IOException
	 */
	static void testPut(HTableInterface hTable) throws IOException {
		// 单个put
		Put put = new Put(Bytes.toBytes("row1"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("11"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("age"), Bytes.toBytes(27));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("phone"), Bytes.toBytes("021-11111111"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("email"), Bytes.toBytes("zhangsan@qq.com"));
		hTable.put(put);

		// 同时put多个
		Put put1 = new Put(Bytes.toBytes("row2"));
		put1.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("2"));
		put1.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("user2"));

		Put put2 = new Put(Bytes.toBytes("row3"));
		put2.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("3"));
		put2.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("user3"));

		Put put3 = new Put(Bytes.toBytes("row4"));
		put3.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("4"));
		put3.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("user4"));

		List<Put> list = new ArrayList<Put>();
		list.add(put1);
		list.add(put2);
		list.add(put3);
		hTable.put(list);

		// 检测put,条件成功就插入，要求rowkey是一样的。
		Put put4 = new Put(Bytes.toBytes("row5"));
		put4.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("7"));
		hTable.checkAndPut(Bytes.toBytes("row5"), Bytes.toBytes("f"), Bytes.toBytes("id"), null, put4);
		System.out.println("插入成功");
	}

	/**
	 * 测试get命令
	 * 
	 * @param hTable
	 * @throws IOException
	 */
	static void testGet(HTableInterface hTable) throws IOException {
		Get get = new Get(Bytes.toBytes("row1"));
		Result result = hTable.get(get);
		byte[] buf = result.getValue(family, Bytes.toBytes("id"));
		System.out.println("id:" + Bytes.toString(buf));
		buf = result.getValue(family, Bytes.toBytes("age"));
		System.out.println("age:" + Bytes.toInt(buf));
		buf = result.getValue(family, Bytes.toBytes("name"));
		System.out.println("name:" + Bytes.toString(buf));
		buf = result.getRow();
		System.out.println("row:" + Bytes.toString(buf));
	}

	/**
	 * 测试delete
	 * 
	 * @param hTable
	 * @throws IOException
	 */
	static void testDelete(HTableInterface hTable) throws IOException {
		Delete delete = new Delete(Bytes.toBytes("row3"));
		// 删除列
		delete = delete.deleteColumn(family, Bytes.toBytes("id"));
		// 直接删除family
		// delete.deleteFamily(family);
		hTable.delete(delete);
		System.out.println("删除成功");
	}

}

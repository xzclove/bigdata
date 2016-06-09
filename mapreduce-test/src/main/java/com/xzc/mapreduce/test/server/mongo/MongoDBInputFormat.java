package com.xzc.mapreduce.test.server.mongo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * @Des 自定义MongoDB Inputformat
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月3日 下午3:40:24
 * @Version V1.0.0
 */
public class MongoDBInputFormat<V extends MongoDBWritable> extends InputFormat<LongWritable, V> {

	/**
	 * MongoDB自定义InputSplit
	 * 
	 * @author gerry       
	 */
	static class MongoDBInputSplit extends InputSplit implements Writable {
		// [start,end)
		private long start; // 起始位置，包含
		private long end; // 终止位置，不包含

		public MongoDBInputSplit() {   
			super();
		}

		public MongoDBInputSplit(long start, long end) {
			super();
			this.start = start;
			this.end = end;
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return end - start;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[0];
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(this.start);
			out.writeLong(this.end);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.start = in.readLong();
			this.end = in.readLong();
		}

	}

	/**
	 * 获取分片信息
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		// 获取mongo连接
		DB mongo = Mongo.connect(new DBAddress("127.0.0.1", "hadoop"));
		// 设置用户名密码
		// mongo.authenticateCommand(username, password);
		// 获取mongo集合
		DBCollection dbCollection = mongo.getCollection("persons");

		// 每两条数据一个mapper，块大小
		int chunkSize = 2;
		long size = dbCollection.count(); // 获取mongodb对于collection的数据条数，假如 7条数据
		long chunk = size / chunkSize; // 计算mapper个数 3

		List<InputSplit> list = new ArrayList<InputSplit>();
		for (int i = 0; i < chunk; i++) {
			// 0 1 2
			if (i + 1 == chunk) {
				list.add(new MongoDBInputSplit(i * chunkSize, size)); // i = 2
																		// [4,7)
			} else {
				list.add(new MongoDBInputSplit(i * chunkSize, i * chunkSize + chunkSize)); // [0,2)
																							// [2,4)
			}
		}
		return list;
	}

	/**
	 * 获取具体的reader类
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public RecordReader<LongWritable, V> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new MongoDBRecordReader(split, context);
	}

	/**
	 * 一个空的mongodb自定义数据类型
	 * 
	 * @author gerry
	 *
	 */
	static class NullMongoDBWritable implements MongoDBWritable {

		@Override
		public void write(DataOutput out) throws IOException {
		}

		@Override
		public void readFields(DataInput in) throws IOException {
		}

		@Override
		public void readFields(DBObject dbObject) {
		}

		@Override
		public void write(DBCollection dbCollection) {
		}
	}

	/**
	 * 自定义mongo reader类
	 * 
	 * @author gerry
	 *
	 * @param <V>
	 */
	static class MongoDBRecordReader<V extends MongoDBWritable> extends RecordReader<LongWritable, V> {

		private MongoDBInputSplit split;
		private int index;
		private DBCursor dbCursor;
		private LongWritable key;
		private V value;

		public MongoDBRecordReader() {
			super();
		}

		public MongoDBRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
				InterruptedException {
			super();
			this.initialize(split, context);
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			this.split = (MongoDBInputSplit) split;
			Configuration conf = context.getConfiguration();
			key = new LongWritable();
			Class clz = conf.getClass("mapreduce.mongo.split.value.class", NullMongoDBWritable.class);
			value = (V) ReflectionUtils.newInstance(clz, conf);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (this.dbCursor == null) {
				// 获取mongo连接
				DB mongo = Mongo.connect(new DBAddress("127.0.0.1", "hadoop"));
				// 获取mongo集合
				DBCollection dbCollection = mongo.getCollection("persons");
				// 获取DBcurstor对象
				dbCursor = dbCollection.find().skip((int) this.split.start).limit((int) this.split.getLength());
			}
			boolean hasNext = this.dbCursor.hasNext();
			if (hasNext) {
				DBObject dbObject = this.dbCursor.next();
				this.key.set(this.split.start + index);
				this.index++;
				this.value.readFields(dbObject);
			}
			return hasNext;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return this.key;
		}

		@Override
		public V getCurrentValue() throws IOException, InterruptedException {
			return this.value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public void close() throws IOException {
			this.dbCursor.close();
		}

	}

}

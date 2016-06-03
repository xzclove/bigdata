package com.xzc.mapreduce.test.mongo; 
import org.apache.hadoop.io.Writable;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
/** 
 * @Des  mongodb自定义数据类型
 * @Author feelingxu@tcl.com: 
 * @Date 创建时间：2016年6月3日 下午3:42:13 
 * @Version V1.0.0
 */
public interface MongoDBWritable extends Writable {
	/**
	 * 从mongodb中读取数据
	 * 
	 * @param dbObject
	 */
	public void readFields(DBObject dbObject);

	/**
	 * 往mongodb中写入数据
	 * 
	 * @param dbCollection
	 */
	public void write(DBCollection dbCollection);

}
 
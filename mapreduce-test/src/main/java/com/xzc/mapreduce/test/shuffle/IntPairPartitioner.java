package com.xzc.mapreduce.test.shuffle;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class IntPairPartitioner extends Partitioner<IntPair, IntWritable> {

	@Override
	public int getPartition(IntPair key, IntWritable value, int numPartitions) {
		if (numPartitions >= 2) {
			int first = key.getFirst();
			if (first % 2 == 0) {
				// 是偶数，需要第二个reducer进行处理
				return 1;
			} else {
				// 是奇数，所以需要第一个reducer进行处理。返回值是从0到num-1的一个范围。
				return 0;
			}
		} else {
			throw new IllegalArgumentException("reducer个数必须大于1");
		}
	}

}

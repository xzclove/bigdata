package com.xzc.mapreduce.test.shuffle;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Des 自定义输出key数据类型
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月3日 下午3:40:24
 * @Version V1.0.0
 */
public class IntPairGrouping extends WritableComparator {
	public IntPairGrouping() {
		super(IntPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntPair key1 = (IntPair) a;
		IntPair key2 = (IntPair) b;
		return Integer.compare(key1.getFirst(), key2.getFirst());
	}
}

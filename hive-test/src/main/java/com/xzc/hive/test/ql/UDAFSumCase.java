package com.xzc.hive.test.ql;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

/**
 * 自定义UDAF函数实现
 * 
 * @author gerry
 *
 */
@SuppressWarnings("deprecation")
public class UDAFSumCase extends AbstractGenericUDAFResolver {
	@Override
	public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
		if (info.isAllColumns()) {
			// 函数允许使用“*”查询的时候会返回true。
			throw new SemanticException("不支持使用*查询");
		}
		// 获取函数参数列表
		ObjectInspector[] inspectors = info.getParameterObjectInspectors();
		if (inspectors.length != 1) {
			throw new UDFArgumentException("只支持一个参数进行查询");
		}
		AbstractPrimitiveWritableObjectInspector apwoi = (AbstractPrimitiveWritableObjectInspector) inspectors[0];
		switch (apwoi.getPrimitiveCategory()) {
		case BYTE:
		case INT:
		case SHORT:
		case LONG:
			// 都是进行整修的sum操作
			return new SumLongEvaluator();
		case FLOAT:
		case DOUBLE:
			// 进行浮点型的sum操作
			return new SumDoubleEvaluator();
		default:
			throw new UDFArgumentException("参数异常");
		}
	}

	/**
	 * 进行浮点型操作
	 * 
	 * @author gerry
	 *
	 */
	static class SumDoubleEvaluator extends GenericUDAFEvaluator {
		private PrimitiveObjectInspector inputOI;

		static class SumDoubleAgg implements AggregationBuffer {
			double sum;
			boolean empty;
		}

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			super.init(m, parameters);
			this.inputOI = (PrimitiveObjectInspector) parameters[0];
			return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			SumDoubleAgg sda = new SumDoubleAgg();
			this.reset(sda);
			return sda;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			SumDoubleAgg sda = (SumDoubleAgg) agg;
			sda.empty = true;
			sda.sum = 0;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			this.merge(agg, parameters[0]);
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			return this.terminate(agg);
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			if (partial != null) {
				SumDoubleAgg sda = (SumDoubleAgg) agg;
				sda.sum += PrimitiveObjectInspectorUtils.getDouble(partial, inputOI);
				sda.empty = false;
			}
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			SumDoubleAgg sda = (SumDoubleAgg) agg;
			if (sda.empty) {
				return null;
			}
			return new DoubleWritable(sda.sum);
		}

	}

	/**
	 * 进行整形的sum操作
	 * 
	 * @author gerry
	 *
	 */
	static class SumLongEvaluator extends GenericUDAFEvaluator {
		private PrimitiveObjectInspector inputOI;

		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
			super.init(m, parameters);
			if (parameters.length != 1) {
				throw new UDFArgumentException("参数异常");
			}
			inputOI = (PrimitiveObjectInspector) parameters[0];
			return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
		}

		/**
		 * 自定义类型
		 * 
		 * @author gerry
		 *
		 */
		static class SumLongAgg implements AggregationBuffer {
			long sum;
			boolean empty;
		}

		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			SumLongAgg sla = new SumLongAgg();
			this.reset(sla);
			return sla;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			SumLongAgg sla = (SumLongAgg) agg;
			sla.sum = 0;
			sla.empty = true;
		}

		/**
		 * 循环处理会调用的方法
		 */
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
			if (parameters.length != 1) {
				throw new UDFArgumentException("参数异常");
			}
			this.merge(agg, parameters[0]);
		}

		/**
		 * 部分聚合后的数据输出
		 * 
		 */
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			return this.terminate(agg);
		}

		/**
		 * 合并操作
		 */
		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			if (partial != null) {
				SumLongAgg sla = (SumLongAgg) agg;
				sla.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI);
				sla.empty = false;
			}
		}

		/**
		 * 全部输出
		 */
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			SumLongAgg sla = (SumLongAgg) agg;
			if (sla.empty) {
				return null;
			}
			return new LongWritable(sla.sum);

		}

	}
}

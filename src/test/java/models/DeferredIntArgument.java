package models;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.io.IntWritable;

public class DeferredIntArgument implements DeferredObject {

	private IntWritable iArgument = null;

	public DeferredIntArgument(IntWritable iArgument) {
		this.iArgument = iArgument;
	}

	public void prepare(int version) throws HiveException {
	}

	public Object get() throws HiveException {
		return iArgument;
	}
}

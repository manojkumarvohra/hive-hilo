package models;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.io.LongWritable;

public class DeferredLongArgument implements DeferredObject {

	private LongWritable lArgument = null;

	public DeferredLongArgument(LongWritable lArgument) {
		this.lArgument = lArgument;
	}

	public void prepare(int version) throws HiveException {
	}

	public Object get() throws HiveException {
		return lArgument;
	}

}

package models;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

public class DeferredArgument<T> implements DeferredObject {
	
	private T argument = null;

	public DeferredArgument(T argument) {
		this.argument = argument;
	}

	@Override
	public void prepare(int version) throws HiveException {
	}

	@Override
	public Object get() throws HiveException {
		return argument;
	}

}

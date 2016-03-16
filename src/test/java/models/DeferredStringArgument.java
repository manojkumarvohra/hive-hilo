package models;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

public class DeferredStringArgument implements DeferredObject {

	private String sArgument = null;

	public DeferredStringArgument(String sequenceName) {
		this.sArgument = sequenceName;
	}

	public void prepare(int version) throws HiveException {
	}

	public Object get() throws HiveException {
		return sArgument;
	}
}

/**************************
*@Author: Manoj Kumar Vohra
*@Created: 16-02-2016
**************************/
package com.bigdata.hive.udf;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import com.bigdata.curator.Incrementer;

@UDFType(deterministic = false, stateful = true)
public class SequenceGenerator extends GenericUDF {

	private static String zookeeperAddress = null;
	private static Map<String, SequenceState> sequenceStateMap = new HashMap<String, SequenceState>();

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		if (arguments.length != 1) {
			throw new UDFArgumentLengthException("please provide the sequence name: Usage => FunctionName(String)");
		}

		ObjectInspector sequenceName = arguments[0];

		if (!(sequenceName instanceof StringObjectInspector)) {
			throw new UDFArgumentException("sequencename argument must be a string");
		}

		Properties udfProperties = new Properties();
		InputStream inputStream = null;

		try {

			udfProperties.load(SequenceGenerator.class.getResourceAsStream("/UDFProperties.properties"));
			zookeeperAddress = udfProperties.getProperty("zookeeperaddress");
		} catch (IOException ex) {
			throw new RuntimeException("Unable to load UDF properties." + ex.getMessage());
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		Object sequenceName = arguments[0].get();

		if (sequenceName == null) {
			throw new UDFArgumentException("sequencename cannot be null");
		}

		String sequenceNamePath = PrimitiveObjectInspectorFactory.javaStringObjectInspector
				.getPrimitiveJavaObject(sequenceName);

		SequenceState sequenceState = sequenceStateMap.get(sequenceNamePath);

		try {

			if (sequenceState == null) {
				Incrementer incrementer = new Incrementer(zookeeperAddress, "/" + sequenceNamePath);
				sequenceState = new SequenceState();
				sequenceState.setIncrementer(incrementer);
				sequenceStateMap.put(sequenceNamePath, sequenceState);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error communicating with zookeeper: " + e.getMessage());
		}

		try {

			if (sequenceState.getCounter() == null || sequenceState.getCounter() == sequenceState.getEndValue()) {
				sequenceState.resetCounters();
				return sequenceState.getCounter();
			}

			sequenceState.incrementCounter();

		} catch (Exception e) {
			throw new RuntimeException("Error executing UDF: " + e.getMessage());
		}

		return sequenceState.getCounter();
	}

	@Override
	public String getDisplayString(String[] children) {
		return "Sequence generator function: returns a unique incrementing sequence value";
	}

	public void destroy() {
		for (String sequenceName : sequenceStateMap.keySet()) {
			SequenceState sequenceState = sequenceStateMap.get(sequenceName);
			if (sequenceState != null) {
				sequenceState.getIncrementer().removeCounter();
			}
		}

	}
}

package com.bigdata.hive.udf.impl;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;

import com.bigdata.hive.udf.def.SequenceCleaner;
import com.bigdata.udf.util.HLSequenceIncrementer;

@UDFType(deterministic = false, stateful = true)
public class HLSequenceCleaner extends GenericUDF implements SequenceCleaner {

	private static String zookeeperAddress = null;
	private static Properties udfProperties = null;
	private String sequenceName = null;
	private HLSequenceIncrementer incrementer = null;
	private boolean evaluationStarted = false;
	private transient Logger logger = Logger.getLogger(this.getClass());

	@Override
	public String getDisplayString(String[] children) {
		return "Sequence cleaner function: cleans the provided hilo sequence";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		if (arguments.length != 1) {
			throw new UDFArgumentLengthException(
					"please provide only the sequence name: Usage => FunctionName(<String> sequenceName)");
		}

		ObjectInspector sequenceName = arguments[0];

		if (!(sequenceName instanceof StringObjectInspector)) {
			throw new UDFArgumentException("sequencename argument must be a string");
		}

		udfProperties = new Properties();

		try {
			udfProperties.load(this.getClass().getResourceAsStream("/UDFProperties.properties"));
			zookeeperAddress = udfProperties.getProperty("zookeeperaddress");
		} catch (IOException ex) {
			String errorMessage = "Unable to load UDF properties." + ex.getMessage();
			logger.error(errorMessage);
			throw new RuntimeException(errorMessage);
		}

		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		if (!evaluationStarted) {

			Object sequenceParam = arguments[0].get();

			if (sequenceParam == null) {
				throw new UDFArgumentException("sequencename cannot be null");
			}

			sequenceName = PrimitiveObjectInspectorFactory.javaStringObjectInspector
					.getPrimitiveJavaObject(sequenceParam);

			if (sequenceName.startsWith("/")) {
				throw new UDFArgumentException("sequencename can't start with /");
			}

			logger.warn("Starting the deletion of sequence:" + sequenceName);

			try {
				incrementer = new HLSequenceIncrementer(zookeeperAddress,
						HLSequenceGenerator.zooKeeperSequenceRoot + sequenceName);

			} catch (Exception exception) {
				String errorMessage = "Error communicating with zookeeper: " + exception.getMessage();
				logger.error(errorMessage);
				throw new RuntimeException(errorMessage);
			}
			evaluationStarted = true;

			return clean();
		}

		return "Please use a single row execution for this function to avoid performance problems\n"
				+ "Example usage could be:\n SELECT FUNCTIONNAME(sequenceName);";
	}

	@Override
	public String clean() {

		try {

			if (incrementer.checkSequenceNotAvailable()) {
				return "Sequence path:[" + HLSequenceGenerator.zooKeeperSequenceRoot + sequenceName
						+ "] does not exists";
			}

			incrementer.removeSequencePath();

			if (incrementer.checkSequenceNotAvailable()) {

				String success_message = "successfully deleted sequence:" + sequenceName;
				logger.info(success_message);
				return success_message;
			}
		} catch (Exception exception) {
			String errorMessage = "Error deleting sequence: " + exception.getMessage();
			logger.error(errorMessage);
			throw new RuntimeException(errorMessage);
		}

		return "Failed to delete sequence:" + sequenceName;
	}

}

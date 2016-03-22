package com.bigdata.hive.udf.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.log4j.Logger;

import com.bigdata.hive.udf.def.SequenceGenerator;
import com.bigdata.udf.util.HLSequenceIncrementer;

@UDFType(deterministic = false, stateful = true)
public class HLSequenceGenerator extends GenericUDF implements SequenceGenerator {

	private static final String LOW_SUFFIX = ".low";
	private static final String SEED_SUFFIX = ".seed";
	private static final int DEFAULT_LOW_VALUE = 200;

	protected static String zooKeeperSequenceRoot = "/sequences/hl/";
	private static String zookeeperAddress = null;
	private static Properties udfProperties = null;
	private HLSequenceState sequenceState = null;
	private boolean evaluationStarted = false;
	private String sequenceNameParam = null;
	private Integer lowValueParam = null;
	private Long seedValueParam = null;

	private transient Logger logger = Logger.getLogger(this.getClass());

	@Override
	public String getDisplayString(String[] children) {
		return "Sequence generator function: returns a unique incrementing sequence value";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		switch (arguments.length) {

		case 1:
			verifySequenceNameInspector(arguments);
			break;

		case 2:
			verifySequenceNameInspector(arguments);
			verifyLowValueInspector(arguments);
			break;

		case 3:
			verifySequenceNameInspector(arguments);
			verifyLowValueInspector(arguments);
			verifySeedValueInspector(arguments);
			break;

		default:
			throw new UDFArgumentLengthException(
					"Invalid function usage: Correct Usage => FunctionName(<String> sequenceName, <int> lowvalue[optional], <long> seedvalue[optional])");
		}

		udfProperties = new Properties();
		InputStream inputStream = null;

		try {
			udfProperties.load(HLSequenceGenerator.class.getResourceAsStream("/UDFProperties.properties"));
			zookeeperAddress = udfProperties.getProperty("zookeeperaddress");
		} catch (IOException ex) {
			String errorMessage = "Unable to load UDF properties." + ex.getMessage();
			logger.error(errorMessage);
			throw new RuntimeException(errorMessage);
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					logger.warn("Unable to close properties stream." + e.getMessage());
				}
			}
		}

		return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		if (!evaluationStarted) {
			switch (arguments.length) {

			case 1:
				sequenceNameParam = checkSequenceNameArgument(arguments);
				break;
			case 2:
				sequenceNameParam = checkSequenceNameArgument(arguments);
				lowValueParam = checkLowValueArgument(arguments);
				break;
			case 3:
				sequenceNameParam = checkSequenceNameArgument(arguments);
				lowValueParam = checkLowValueArgument(arguments);
				seedValueParam = checkSeedValueArgument(arguments);
				break;
			}
		}

		return next();
	}

	public Long next() {
		return hlNextImpl(sequenceNameParam, lowValueParam, seedValueParam);
	}

	public Long hlNextImpl(String sequenceNamePath, Integer iLowValue, Long lSeedValue) {

		Long seedValue = lSeedValue;
		Integer seqLowValue = iLowValue;
		Long startHIValue = null;

		/*
		 * If in-line seed value and low value are not provided while invoking
		 * function, these values would be fetched from properties file. If
		 * properties also are not configured than default values would be used
		 */
		if (!evaluationStarted) {
			try {

				if (sequenceState == null) {

					sequenceState = new HLSequenceState();

					if (seqLowValue == null) {
						String seqLowValueProperty = udfProperties.getProperty(sequenceNamePath + LOW_SUFFIX);
						seqLowValue = seqLowValueProperty != null ? Integer.parseInt(seqLowValueProperty.trim())
								: DEFAULT_LOW_VALUE;
					}
					sequenceState.setLoValue(seqLowValue);

					if (seedValue == null) {
						String seedValueProperty = udfProperties.getProperty(sequenceNamePath + SEED_SUFFIX);
						if (seedValueProperty != null) {
							seedValue = Long.parseLong(seedValueProperty.trim());
							startHIValue = seedValue / seqLowValue - 1;
						}
					} else {
						startHIValue = seedValue / seqLowValue - 1;
					}

					HLSequenceIncrementer incrementer = new HLSequenceIncrementer(zookeeperAddress,
							zooKeeperSequenceRoot + sequenceNamePath);
					incrementer.createCounter(startHIValue);
					sequenceState.setIncrementer(incrementer);
				}
			} catch (Exception e) {
				String errorMessage = "Error communicating with zookeeper: " + e.getMessage();
				logger.error(errorMessage);
				throw new RuntimeException(errorMessage);
			}
			evaluationStarted = true;
		}

		try {

			if (sequenceState.getCounter() == null || sequenceState.getCounter() >= sequenceState.getEndValue()) {
				sequenceState.resetCounters(seedValue, startHIValue);
				return sequenceState.getCounter();
			}

			sequenceState.incrementCounter();

		} catch (Exception e) {
			String errorMessage = "Error executing UDF: " + e.getMessage();
			logger.error(errorMessage);
			throw new RuntimeException(errorMessage);
		}

		return sequenceState.getCounter();

	}

	private String checkSequenceNameArgument(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object sequenceName = arguments[0].get();

		if (sequenceName == null) {
			throw new UDFArgumentException("sequencename cannot be null");
		}

		String sequenceNamePath = PrimitiveObjectInspectorFactory.javaStringObjectInspector
				.getPrimitiveJavaObject(sequenceName);

		if (sequenceNamePath.startsWith("/")) {
			throw new UDFArgumentException("sequencename can't start with /");
		}

		return sequenceNamePath;
	}

	private Integer checkLowValueArgument(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object lowValue = arguments[1].get();

		if (lowValue == null) {
			throw new UDFArgumentException("Low value cannot be null");
		}

		Integer lowValueParam = (Integer) PrimitiveObjectInspectorFactory.writableIntObjectInspector
				.getPrimitiveJavaObject(lowValue);
		if (lowValueParam <= 0) {
			throw new UDFArgumentException("Low value should be greater than 0");
		}

		return lowValueParam;
	}

	private Long checkSeedValueArgument(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object seedValue = arguments[2].get();

		if (seedValue == null) {
			throw new UDFArgumentException("Seed value cannot be null");
		}

		Long seedValueParam = (Long) PrimitiveObjectInspectorFactory.writableLongObjectInspector
				.getPrimitiveJavaObject(seedValue);

		if (seedValueParam < 0) {
			throw new UDFArgumentException("Seed value can't be negative");
		}

		return seedValueParam;
	}

	private void verifySeedValueInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector seedValueInspector = arguments[2];

		if (!(seedValueInspector instanceof LongObjectInspector)) {
			throw new UDFArgumentException("Seed value argument must be a long");
		}
	}

	private void verifyLowValueInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector lowValueInspector = arguments[1];

		if (!(lowValueInspector instanceof IntObjectInspector)) {
			throw new UDFArgumentException("Low value argument must be an integer");
		}
	}

	private void verifySequenceNameInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector sequenceNameInspector = arguments[0];
		if (!(sequenceNameInspector instanceof StringObjectInspector)) {
			throw new UDFArgumentException("sequencename argument must be a string");
		}
	}

	public void destroy() {
		if (sequenceState != null) {
			sequenceState.getIncrementer().removeSequencePath();
		}
	}
}
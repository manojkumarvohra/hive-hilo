package integration;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Properties;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Before;
import org.junit.Test;

import com.bigdata.hive.udf.impl.HLSequenceCleaner;
import com.bigdata.udf.util.HLSequenceIncrementer;

import models.DeferredArgument;

/*
 * NOTE: This integration test requires a valid zookeeper instance to be provided.
 * The sequence created by test is cleaned after test completes.
 */
public class HLSequenceCleanerIntegrationTest {

	HLSequenceCleaner cleaner = new HLSequenceCleaner();
	HLSequenceIncrementer incrementer;
	private String zooKeeperSequenceRoot = "/sequences/hl/";
	private String sequenceName = "test-cleaner-sequence";

	@Before
	public void setUp() throws Exception {

		Properties udfProperties = new Properties();
		udfProperties.load(this.getClass().getResourceAsStream("/UDFProperties.properties"));
		String zookeeperAddress = udfProperties.getProperty("zookeeperaddress");
		incrementer = new HLSequenceIncrementer(zookeeperAddress, zooKeeperSequenceRoot + sequenceName);
	}

	@Test
	public void shouldClearSequence() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[1];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		cleaner.initialize(objectInspector);

		assertThat(incrementer.checkSequenceNotAvailable(), is(true));
		incrementer.createCounter(1L);
		assertThat(incrementer.checkSequenceNotAvailable(), is(false));

		DeferredObject[] arguments = new DeferredObject[1];
		arguments[0] = new DeferredArgument<String>(sequenceName);

		String result = (String) cleaner.evaluate(arguments);
		assertThat(incrementer.checkSequenceNotAvailable(), is(true));
		assertThat(result, is("successfully deleted sequence:" + sequenceName));
	}
}

package com.bigdata.hive.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.is;

public class SequenceGeneratorTest {

	SequenceGenerator sequenceGenerator = new SequenceGenerator();

	@Test
	public void shouldGenerateSequences() throws HiveException {

		ObjectInspector[] objectInspector = new ObjectInspector[1];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		sequenceGenerator.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[1];
		arguments[0] = new DeferredArgument("testSequence");

		int loopCounter = 0;
		List<Long> counters = new ArrayList<Long>();
		List<Long> expected = new ArrayList<Long>();
		Long counter=0L;
		while (loopCounter < 200) {
			counters.add((Long) sequenceGenerator.evaluate(arguments));
			expected.add(counter);
			counter++;
			loopCounter++;
		}
		
		assertThat(counters.size(), is(200));
		assertTrue(expected.containsAll(counters));
	}

	@After
	public void tearDown() throws IOException {
		sequenceGenerator.destroy();
		sequenceGenerator.close();		
	}

	static class DeferredArgument implements DeferredObject {

		private String sequenceName = null;

		public DeferredArgument(String sequenceName) {
			this.sequenceName = sequenceName;
		}

		public void prepare(int version) throws HiveException {
		}

		public Object get() throws HiveException {
			return sequenceName;
		}

	}

}

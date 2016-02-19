package com.bigdata.hive.udf;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.After;
import org.junit.Test;

public class HLSequenceGeneratorTest {

	HLSequenceGenerator_v01 sequenceGeneratorForJVM1 = new HLSequenceGenerator_v01();
	HLSequenceGenerator_v01 sequenceGeneratorForJVM2 = new HLSequenceGenerator_v01();
	
	@Test
	public void shouldGenerateSequences() throws HiveException, InterruptedException {

		ObjectInspector[] objectInspector = new ObjectInspector[1];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		sequenceGeneratorForJVM1.initialize(objectInspector);
		sequenceGeneratorForJVM2.initialize(objectInspector);
		
		List<Long> counters = new ArrayList<Long>();
		List<Long> expectedCounters = new ArrayList<Long>();
		
		CountDownLatch countDownLatch = new CountDownLatch(2);
		new Thread(new Evaluation(sequenceGeneratorForJVM1, counters, countDownLatch)).start();
		new Thread(new Evaluation(sequenceGeneratorForJVM2, counters, countDownLatch)).start();
		
		countDownLatch.await();
		
		for(long i=327; i<529; i++){
			expectedCounters.add(i);
		}
		for(long i=1000; i<1203; i++){
			expectedCounters.add(i);
		}
		assertThat(counters.size(), is(404));
		assertTrue(expectedCounters.containsAll(counters));
	}

	@After
	public void tearDown() throws IOException {
		sequenceGeneratorForJVM1.destroy();
		sequenceGeneratorForJVM1.close();
		sequenceGeneratorForJVM2.close();
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

	static class Evaluation implements Runnable {

		HLSequenceGenerator_v01 sequenceGenerator = null;
		List<Long> counters = null;
		CountDownLatch countDownLatch = null;

		public Evaluation(HLSequenceGenerator_v01 sequenceGenerator, List<Long> counters,
				CountDownLatch countDownLatch) {
			this.sequenceGenerator = sequenceGenerator;
			this.counters = counters;
			this.countDownLatch = countDownLatch;
		}

		public void run() {
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}

			DeferredObject[] arguments = new DeferredObject[1];
			arguments[0] = new DeferredArgument("testcheckseq");

			int loopCounter = 0;
			while (loopCounter < 202) {
				try {
					counters.add((Long) sequenceGenerator.evaluate(arguments));
				} catch (HiveException e) {
				}
				loopCounter++;
			}

			countDownLatch.countDown();

		}

	}

}

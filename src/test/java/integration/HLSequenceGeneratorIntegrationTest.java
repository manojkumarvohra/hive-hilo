package integration;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.After;
import org.junit.Test;

import com.bigdata.hive.udf.HLSequenceGenerator;

import models.DeferredArgument;

/*
 * NOTE: This integration test requires a valid zookeeper instance to be provided.
 * The sequence created by test is cleaned after test completes.
 */
public class HLSequenceGeneratorIntegrationTest {

	HLSequenceGenerator sequenceGeneratorForJVM1 = new HLSequenceGenerator();
	HLSequenceGenerator sequenceGeneratorForJVM2 = new HLSequenceGenerator();
	
	@Test
	public void shouldGenerateSequences() throws HiveException, InterruptedException {

		ObjectInspector[] objectInspector = new ObjectInspector[3];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
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

	static class Evaluation implements Runnable {

		HLSequenceGenerator sequenceGenerator = null;
		List<Long> counters = null;
		CountDownLatch countDownLatch = null;

		public Evaluation(HLSequenceGenerator sequenceGenerator, List<Long> counters,
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

			DeferredObject[] arguments = new DeferredObject[3];
			arguments[0] = new DeferredArgument<String>("testSpeedCheckSequence");
			arguments[1] = new DeferredArgument<IntWritable>(new IntWritable(500));
			arguments[2] = new DeferredArgument<LongWritable>(new LongWritable(327L));

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

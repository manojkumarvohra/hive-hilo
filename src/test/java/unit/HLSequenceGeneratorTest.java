package unit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

import com.bigdata.hive.udf.HLSequenceGenerator;

import models.DeferredArgument;

public class HLSequenceGeneratorTest {

	private HLSequenceGenerator sequenceGenerator = new HLSequenceGenerator();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);

	}

	@Test
	public void shouldGetCorrectDisplayString() throws Exception {

		assertThat(sequenceGenerator.getDisplayString(null),
				is("Sequence generator function: returns a unique incrementing sequence value"));
	}

	@Test
	public void shouldThrowExceptionForNoArgumentsPassed() throws Exception {
		expectedException.expect(UDFArgumentLengthException.class);
		expectedException.expectMessage(
				"Invalid function usage: Correct Usage => FunctionName(<String> sequenceName, <int> lowvalue[optional, <long> seedvalue[optional])");

		sequenceGenerator.initialize(new ObjectInspector[0]);
	}

	@Test
	public void shouldThrowExceptionForMoreThan3ArgumentsPassed() throws Exception {
		expectedException.expect(UDFArgumentLengthException.class);
		expectedException.expectMessage(
				"Invalid function usage: Correct Usage => FunctionName(<String> sequenceName, <int> lowvalue[optional, <long> seedvalue[optional])");

		sequenceGenerator.initialize(new ObjectInspector[4]);
	}

	@Test
	public void shouldThrowExceptionIfSequenceNameIsNull() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("sequencename cannot be null");

		DeferredObject[] arguments = new DeferredObject[1];
		arguments[0] = new DeferredArgument<String>(null);

		sequenceGenerator.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfSequenceNameIsNotString() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("sequencename argument must be a string");

		ObjectInspector[] objectInspector = new ObjectInspector[1];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

		sequenceGenerator.initialize(objectInspector);
	}

	@Test
	public void shouldThrowExceptionIfSequenceNameStartsWithSlash() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("sequencename can't start with /");

		DeferredObject[] arguments = new DeferredObject[1];
		arguments[0] = new DeferredArgument<String>("/InvalidSequence");

		sequenceGenerator.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfLowValueIsNull() throws Exception {
		expectedException.expect(UDFArgumentException.class);

		expectedException.expectMessage("Low value cannot be null");
		DeferredObject[] arguments = new DeferredObject[2];
		arguments[0] = new DeferredArgument<String>("validSequence");
		arguments[1] = new DeferredArgument<IntWritable>(null);

		sequenceGenerator.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfLowValueIsNotInt() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Low value argument must be an integer");

		ObjectInspector[] objectInspector = new ObjectInspector[2];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		sequenceGenerator.initialize(objectInspector);
	}

	@Test
	public void shouldThrowExceptionIfLowValueIs0() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Low value should be greater than 0");

		DeferredObject[] arguments = new DeferredObject[2];
		arguments[0] = new DeferredArgument<String>("validSequence");
		arguments[1] = new DeferredArgument<IntWritable>(new IntWritable(0));

		sequenceGenerator.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfLowValueIsLessThan0() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Low value should be greater than 0");

		DeferredObject[] arguments = new DeferredObject[2];
		arguments[0] = new DeferredArgument<String>("validSequence");
		arguments[1] = new DeferredArgument<IntWritable>(new IntWritable(-1));

		sequenceGenerator.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfSeedValueIsNull() throws Exception {
		expectedException.expect(UDFArgumentException.class);

		expectedException.expectMessage("Seed value cannot be null");
		DeferredObject[] arguments = new DeferredObject[3];
		arguments[0] = new DeferredArgument<String>("validSequence");
		arguments[1] = new DeferredArgument<IntWritable>(new IntWritable(1));
		arguments[2] = new DeferredArgument<LongWritable>(null);

		sequenceGenerator.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfSeedValueIsNotInt() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Seed value argument must be a long");

		ObjectInspector[] objectInspector = new ObjectInspector[3];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		sequenceGenerator.initialize(objectInspector);
	}

	@Test
	public void shouldThrowExceptionIfSeedValueIsLessThan0() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Seed value can't be negative");

		DeferredObject[] arguments = new DeferredObject[3];
		arguments[0] = new DeferredArgument<String>("validSequence");
		arguments[1] = new DeferredArgument<IntWritable>(new IntWritable(1));
		arguments[2] = new DeferredArgument<LongWritable>(new LongWritable(-1L));

		sequenceGenerator.evaluate(arguments);
	}

}

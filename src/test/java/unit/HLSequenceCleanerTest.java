package unit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.bigdata.hive.udf.impl.HLSequenceCleaner;

import models.DeferredArgument;

public class HLSequenceCleanerTest {

	private HLSequenceCleaner cleaner = new HLSequenceCleaner();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void shouldGetCorrectDisplayString() throws Exception {

		assertThat(cleaner.getDisplayString(null),
				is("Sequence cleaner function: cleans the provided hilo sequence"));
	}

	@Test
	public void shouldThrowExceptionForNoArgumentsPassed() throws Exception {
		expectedException.expect(UDFArgumentLengthException.class);
		expectedException.expectMessage(
				"please provide only the sequence name: Usage => FunctionName(<String> sequenceName)");

		cleaner.initialize(new ObjectInspector[0]);
	}

	@Test
	public void shouldThrowExceptionForMoreThan3ArgumentsPassed() throws Exception {
		expectedException.expect(UDFArgumentLengthException.class);
		expectedException.expectMessage(
				"please provide only the sequence name: Usage => FunctionName(<String> sequenceName)");

		cleaner.initialize(new ObjectInspector[4]);
	}

	@Test
	public void shouldThrowExceptionIfSequenceNameIsNull() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("sequencename cannot be null");

		DeferredObject[] arguments = new DeferredObject[1];
		arguments[0] = new DeferredArgument<String>(null);

		cleaner.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfSequenceNameIsNotString() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("sequencename argument must be a string");

		ObjectInspector[] objectInspector = new ObjectInspector[1];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

		cleaner.initialize(objectInspector);
	}

	@Test
	public void shouldThrowExceptionIfSequenceNameStartsWithSlash() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("sequencename can't start with /");

		DeferredObject[] arguments = new DeferredObject[1];
		arguments[0] = new DeferredArgument<String>("/InvalidSequence");

		cleaner.evaluate(arguments);
	}

}

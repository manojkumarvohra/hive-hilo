package unit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.bigdata.curator.HLSequenceIncrementer;
import com.bigdata.hive.udf.HLSequenceState;

public class HLSequenceStateTest {

	private HLSequenceState sequenceState = new HLSequenceState();

	@Mock
	private HLSequenceIncrementer incrementer;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		when(incrementer.increment()).thenReturn(0L);
		sequenceState.setIncrementer(incrementer);
		sequenceState.setLoValue(200);
	}

	@Test
	public void shouldMaintainCorrectSequenceStateOnInit() throws Exception {

		sequenceState.resetCounters(327L, -1L);
		assertThat(sequenceState.getCounter(), is(327L));
		assertThat(sequenceState.getHiValue(), is(0L));
		assertThat(sequenceState.getLoValue(), is(200));
		assertThat(sequenceState.getStartValue(), is(sequenceState.getHiValue() * sequenceState.getLoValue()));
		assertThat(sequenceState.getEndValue(), is(sequenceState.getStartValue() + sequenceState.getLoValue() -1));
	}
	
	@Test
	public void shouldIncrementByOne() throws Exception {

		sequenceState.resetCounters(327L, -1L);
		sequenceState.incrementCounter();
		assertThat(sequenceState.getCounter(), is(328L));
		sequenceState.incrementCounter();
		assertThat(sequenceState.getCounter(), is(329L));
		sequenceState.incrementCounter();
		assertThat(sequenceState.getCounter(), is(330L));
	}

}

package com.bigdata.hive.udf;

import com.bigdata.curator.HLSequenceIncrementer;

public class HLSequenceState implements SequenceState {

	private Long counter = null;
	private HLSequenceIncrementer incrementer = null;
	private Long hiValue = null;
	private Integer loValue = null;

	public void resetCounters(Long seedValue, Long startHIValue) throws Exception {
		hiValue = incrementer.increment();
		initCounter(seedValue, startHIValue);
	}

	/*
	 * This is to prevent two JVMs duplicating their start values based on same
	 * seed value. Only one should be allowed based who get the hiCounter
	 * initialized first, rest should start from their batch start value. On
	 * subsequent runs the seed value would not be considered as HiValue in zoo
	 * keeper would have moved ahead.
	 */
	private void initCounter(Long seedValue, Long startHIValue) {

		if (seedValue != null && startHIValue != null && hiValue.equals(startHIValue + 1)) {
			counter = seedValue;
		} else {
			counter = getStartValue();
		}
	}

	public void setIncrementer(HLSequenceIncrementer incrementer) {
		this.incrementer = incrementer;
	}

	public void setLoValue(Integer loValue) {
		this.loValue = loValue;
	}

	public HLSequenceIncrementer getIncrementer() {
		return incrementer;
	}

	public Long getHiValue() {
		return hiValue;
	}

	public Integer getLoValue() {
		return loValue;
	}

	public Long getStartValue() {
		return hiValue * loValue;
	}

	public Long getEndValue() {
		return getStartValue() + loValue - 1;
	}

	public void incrementCounter() {
		counter++;
	}

	@Override
	public Long getCounter() {
		return counter;
	}
}

/**************************
*@Author: Manoj Kumar Vohra
*@Created: 16-02-2016
**************************/
package com.bigdata.hive.udf;

import com.bigdata.curator.Incrementer;

public class SequenceState {

	private Long counter = null;
	private Incrementer incrementer = null;
	private Long currentHiCounter = null;
	private static Long lowCounter = 100L;

	public void resetCounters() throws Exception {
		this.currentHiCounter = this.incrementer.increment();
		this.counter = getStartValue();
	}

	public Incrementer getIncrementer() {
		return incrementer;
	}

	public void setIncrementer(Incrementer incrementer) {
		this.incrementer = incrementer;
	}

	public Long getCounter() {
		return counter;
	}

	public Long getCurrentHiCounter() {
		return currentHiCounter;
	}

	public Long getCurrentLowCounter() {
		return lowCounter;
	}

	public Long getStartValue() {
		return this.currentHiCounter * lowCounter;
	}

	public Long getEndValue() {
		return getStartValue() + lowCounter - 1;
	}

	public void incrementCounter() {
		this.counter++;
	}
}

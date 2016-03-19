package com.bigdata.udf.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.log4j.Logger;

public class HLSequenceIncrementer {

	private String counterPath;
	private CuratorFramework curator;
	private DistributedAtomicLong jvmCounter;
	private transient Logger logger = Logger.getLogger(this.getClass());

	public HLSequenceIncrementer(String zkAddress, String counterPath) throws Exception {
		this.curator = CuratorFrameworkFactory.newClient(zkAddress, new RetryNTimes(5, 1000));
		curator.start();
		this.counterPath = counterPath;
	}

	public void createCounter(Long startHIValue) throws Exception {
		int maximumRetryTimeInMillis = 1000;
		int retryFrequencyInMillis = 100;
		RetryPolicy rp = new RetryUntilElapsed(maximumRetryTimeInMillis, retryFrequencyInMillis);
		RetryPolicy lockPromotionRetryPolicy = new ExponentialBackoffRetry(3, 3);
		PromotedToLock promotedToLock = PromotedToLock.builder().lockPath("/lock").retryPolicy(lockPromotionRetryPolicy)
				.build();

		startHIValue = startHIValue != null ? startHIValue : -1;

		if (checkSequenceNotAvailable()) {
			this.jvmCounter = new DistributedAtomicLong(this.curator, this.counterPath, rp, promotedToLock);
			this.jvmCounter.initialize(startHIValue);
		} else {
			this.jvmCounter = new DistributedAtomicLong(this.curator, this.counterPath, rp, promotedToLock);
		}
	}

	public Long increment() throws Exception {
		Long currentCounter = null;
		try {

			if (this.jvmCounter.get().succeeded()) {

				AtomicValue<Long> incrementState = this.jvmCounter.increment();

				if (incrementState.succeeded()) {
					currentCounter = incrementState.postValue();
				}
			}

		} catch (Exception ex) {
			logger.error("********* INCREMENT COUNTER ERROR: " + ex.getMessage());
			throw new RuntimeException("Error incrementing sequence high counter: " + ex.getMessage());
		}
		return currentCounter;
	}

	public boolean checkSequenceNotAvailable() throws Exception {
		try {
			return curator.checkExists().forPath(this.counterPath) == null;

		} catch (Exception ex) {
			logger.error("********* Error in fetching counter details: " + ex.getMessage());
			throw new RuntimeException("Error in fetching counter details: " + ex.getMessage());
		}
	}

	public void removeSequencePath() {

		try {
			curator.delete().forPath(this.counterPath);
		} catch (Exception e) {
			throw new RuntimeException("Error deleting sequence high counter: " + e.getMessage());
		}

	}

}

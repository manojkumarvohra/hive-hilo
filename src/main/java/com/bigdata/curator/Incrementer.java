/**************************
*@Author: Manoj Kumar Vohra
*@Created: 16-02-2016
**************************/
package com.bigdata.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.log4j.Logger;

public class Incrementer {

	private CuratorFramework curator;
	private String counterPath;
	private DistributedAtomicLong jvmCounter;
	private Logger logger = Logger.getLogger(this.getClass());

	public Incrementer(String zkAddress, String counterPath) throws Exception {
		this.curator = CuratorFrameworkFactory.newClient(zkAddress, new RetryNTimes(5, 1000));
		curator.start();
		this.counterPath = counterPath;
		createCounter();
	}

	private void createCounter() throws Exception {
		int tempoMaximoDeTentativasMilissegundos = 1000;
		int intervaloEntreTentativasMilissegundos = 100;
		RetryPolicy rp = new RetryUntilElapsed(tempoMaximoDeTentativasMilissegundos,
				intervaloEntreTentativasMilissegundos);
		this.jvmCounter = new DistributedAtomicLong(this.curator, this.counterPath, rp);
		this.jvmCounter.initialize((long) -1);
	}

	public Long increment() throws Exception {
		Long currentCounter = null;
		try {

			if (this.jvmCounter.get().succeeded()) {

				if (this.jvmCounter.increment().succeeded()) {
					currentCounter = this.jvmCounter.get().postValue();
				}
			}

		} catch (Exception ex) {
			logger.error("********* INCREMENT COUNTER ERROR: " + ex.getMessage());
			throw new RuntimeException("Error incrementing sequence high counter: " + ex.getMessage());
		}
		return currentCounter;
	}

	public void removeCounter() {

		try {
			curator.delete().forPath(this.counterPath);
		} catch (Exception e) {
			throw new RuntimeException("Error deleting sequence high counter: " + e.getMessage());
		}

	}
}

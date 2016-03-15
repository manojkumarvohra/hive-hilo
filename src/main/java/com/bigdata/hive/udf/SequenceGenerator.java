package com.bigdata.hive.udf;

public interface SequenceGenerator {
	Long next(String sequenceNamePath, Integer iLowValue, Long lSeedValue);
}

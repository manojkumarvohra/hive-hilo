package com.bigdata.hive.udf;

public interface SequenceGenerator {
	
	Long next(String sequenceName);
}

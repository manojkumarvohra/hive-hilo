/*
*
*@Author: Manoj Kumar Vohra
*@Created: 16-02-2016
*
*/
package com.bigdata.hive.udf;

public interface SequenceGenerator {
	
	Long next(String sequenceName);
}

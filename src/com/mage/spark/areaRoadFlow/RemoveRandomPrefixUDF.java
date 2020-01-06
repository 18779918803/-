package com.mage.spark.areaRoadFlow;

import org.apache.spark.sql.api.java.UDF1;

public class RemoveRandomPrefixUDF implements UDF1<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    //1_黄浦区:19
	@Override
	public String call(String val) throws Exception {
		return val.split("_")[1];
	}

}

package com.mage.spark.dao.impl;

import com.mage.spark.constant.Constants;
import com.mage.spark.dao.IWithTheCarDAO;
import com.mage.spark.jdbc.JDBCHelper;
import com.mage.spark.util.DateUtils;

public class WithTheCarDAOImpl implements IWithTheCarDAO {

	@Override
	public void updateTestData(String cars) {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		String sql = "UPDATE task set task_param = ? WHERE task_id = 3";
		Object[] params = new Object[]{"{\"startDate\":[\""+DateUtils.getTodayDate()+"\"],\"endDate\":[\""+DateUtils.getTodayDate()+"\"],\""+ Constants.FIELD_CARS+"\":[\""+cars+"\"]}"};
		jdbcHelper.executeUpdate(sql, params);
	}

}

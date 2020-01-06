package com.mage.spark.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mage.spark.conf.ConfigurationManager;
import com.mage.spark.constant.Constants;

/**
 * 参数工具类
 * @author Administrator
 *
 */
public class ParamUtils {

	/**
	 * 从命令行参数中提取任务id
	 * @param args 命令行参数
	 * @param taskType 参数类型(任务id对应的值是Long类型才可以)，对应my.properties中的key
	 * @return 任务id
	 */
	public static Long getTaskIdFromArgs(String[] args, String taskType) {

		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

		if(local) {
            //如果是本地运行，则从my.properties提取任务ID
		    //spark.local.taskId.monitorFlow
			return ConfigurationManager.getLong(taskType);
		} else {
		    //集群运行，从命令行参数提取任务ID
			try {
				if(args != null && args.length > 0) {
					return Long.valueOf(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}
	
	/**
	 * 从JSON对象中提取参数
	 * @param jsonObject JSON对象
	 * @return 参数
	 * {"name":"zhangsan","age":"18"}
	 */
	public static String getParam(JSONObject jsonObject, String field) {
		JSONArray jsonArray = jsonObject.getJSONArray(field);
		if(jsonArray != null && jsonArray.size() > 0) {
			return jsonArray.getString(0);
		}
		return null;
	}
	
}

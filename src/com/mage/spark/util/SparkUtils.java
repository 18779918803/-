package com.mage.spark.util;

import com.mage.spark.conf.ConfigurationManager;
import com.mage.spark.constant.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.SparkSession;

import com.alibaba.fastjson.JSONObject;
import com.mage.test.MockData;
import scala.Tuple2;


/**
 * Spark工具类
 * @author Administrator
 *
 */
public class SparkUtils {





	
	/**
	 * 根据当前是否本地测试的配置，决定 如何设置SparkConf的master
	 */
	public static void setMaster(SparkConf conf) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		 if(local) {
			conf.setMaster("local");  
		}
	}



	
	/**
	 * 获取SparkSession
	 * 如果是集群运行，就开启hive支持，否则不开启
	 * @return
	 */
	public static SparkSession getSparkSession(SparkConf conf) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			return SparkSession.builder().config(conf).getOrCreate();
		} else {
			return SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();
		}
	}
	
	/**
	 * 生成模拟数据
	 * 如果spark.local配置设置为true，则生成模拟数据；否则不生成
	 */
	public static void mockData(SparkSession sparkSession) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		/**
		 * 如何local为true  说明在本地测试  应该生产模拟数据
		 */
		if(local) {
			MockData.mock(sparkSession);
		}
	}

    /**
     * 将RDD转换成K,V格式的RDD
     *
     * @param cameraRDD
     * @return
     */
    public static JavaPairRDD<String, Row> getMonitor2DetailRDD(JavaRDD<Row> cameraRDD) {
        JavaPairRDD<String, Row> monitorId2Detail = cameraRDD.mapToPair(new PairFunction<Row, String, Row>() {

            /**
             * row.getString(1) 是得到monitor_id 。
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {


                return new Tuple2<>(row.getString(1), row);
            }
        });
        return monitorId2Detail;
    }


    /**
	 * 获取指定日期范围内的卡口信息
	 * @param taskParamsJsonObject  {"startDate":["2018-07-02"],"endDate":["2018-07-02"],"topNum":["5"],"areaName":["黄浦区"]}
	 */
	public static JavaRDD<Row> getCameraRDDByDateRange(SparkSession sparkSession, JSONObject taskParamsJsonObject) {

		String startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE);
		
		String sql = 
				"SELECT * "
				+ "FROM monitor_flow_action "
				+ "WHERE date>='" + startDate + "' "
				+ "AND date<='" + endDate + "'";  
		
		Dataset<Row> monitorDF = sparkSession.sql(sql);

		
		/**
		 * repartition可以提高stage的并行度
		 */
//		return actionDF.javaRDD().repartition(1000);
		return monitorDF.javaRDD();   
	}
	
	/**
	 * 获取指定日期内出现指定车辆的卡扣信息
	 * @param taskParamsJsonObject
	 * @return JavaRDD<Row>
	 */
	public static JavaRDD<Row> getCameraRDDByDateRangeAndCars(SparkSession sparkSession, JSONObject taskParamsJsonObject) {
		String startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE);
		String cars = ParamUtils.getParam(taskParamsJsonObject, Constants.FIELD_CARS);
		String[] carArr = cars.split(",");
		String sql = 
				"SELECT * "
				+ "FROM monitor_flow_action "
				+ "WHERE date>='" + startDate + "' "
				+ "AND date<='" + endDate + "' "
				+ "AND car IN (";

		for (int i = 0; i < carArr.length; i++) {
			sql += "'" + carArr[i] + "'";
			if(i < carArr.length - 1){
				sql += ",";
			}
		}
		sql += ")";
		
		System.out.println("sql:"+sql);
		Dataset<Row> monitorDF = sparkSession.sql(sql);
		
		/**
		 * repartition可以提高stage的并行度
		 */
//		return actionDF.javaRDD().repartition(1000);
		
		return monitorDF.javaRDD();
	}
	
	/*****************************************************/
	/**
	 * 获取指定日期范围和指定区域范围内的卡口信息
     *
	 * @param taskParamsJsonObject 传过来的json对象
	 * @param a 区域
	 */
	public static JavaRDD<Row> getCameraRDDByDateRangeAndArea(SparkSession sparkSession, JSONObject taskParamsJsonObject,String a) {
		String startDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE);
		
		String sql = 
				"SELECT * "
				+ "FROM monitor_flow_action "
				+ "WHERE date>='" + startDate + "' "
				+ "AND date<='" + endDate + "'"
				+ "AND area_id in ('"+a +"')";  
		
		Dataset<Row> monitorDF = sparkSession.sql(sql);
		monitorDF.show();
		/**
		 * repartition可以提高stage的并行度
		 */
//		return actionDF.javaRDD().repartition(1000);
		return monitorDF.javaRDD();   
	}
	
}

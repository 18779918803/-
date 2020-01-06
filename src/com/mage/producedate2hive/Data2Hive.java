package com.mage.producedate2hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

/**
 * 1.代码方式向hive中创建数据
 * 2.后面可以有sql文件执行直接在hive中创建数据库表
 *-Xms800m -Xmx800m  -XX:PermSize=64M -XX:MaxNewSize=256m -XX:MaxPermSize=128m
 */
public class Data2Hive {
	public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("data2hive")
                .master("local")
                .enableHiveSupport()
                .getOrCreate();


        sparkSession.sql("USE traffic");
        sparkSession.sql("DROP TABLE IF EXISTS monitor_flow_action");
		//在hive中创建monitor_flow_action表
        sparkSession.sql("CREATE TABLE IF NOT EXISTS monitor_flow_action "
				+ "(date STRING,monitor_id STRING,camera_id STRING,car STRING,action_time STRING,speed STRING,road_id STRING,area_id STRING) "
				+ "row format delimited fields terminated by '\t' ");
        sparkSession.sql("load data local inpath './monitor_flow_action' into table monitor_flow_action");
		
		//在hive中创建monitor_camera_info表
        sparkSession.sql("DROP TABLE IF EXISTS monitor_camera_info");
        sparkSession.sql("CREATE TABLE IF NOT EXISTS monitor_camera_info (monitor_id STRING, camera_id STRING) row format delimited fields terminated by '\t'");
        sparkSession.sql("LOAD DATA "
				+ "LOCAL INPATH './monitor_camera_info'"
				+ "INTO TABLE monitor_camera_info");
		
		System.out.println("========data2hive finish========");
        sparkSession.stop();
	}
}

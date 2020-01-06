package com.mage.spark.areaRoadFlow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mage.spark.constant.Constants;
import com.mage.spark.context.TrafficContext;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;
import com.mage.spark.conf.ConfigurationManager;

import com.mage.spark.util.ParamUtils;
import com.mage.spark.util.SparkUtils;

import scala.Tuple2;

/**
 * 计算出每一个区域top3的道路流量
 * 每一个区域车流量最多的3条道路   每条道路有多个卡扣
 *
 * @author root
 * ./spark-submit
 * --master spark://hadoop1:7077
 * --executor-memory 512m
 * --driver-class-path /software/Hive/hive-1.2.1/lib/mysql-connector-java-5.1.26-bin.jar:/root/resource/fastjson-1.2.11.jar
 * --jars /software/Hive/hive-1.2.1/lib/mysql-connector-java-5.1.26-bin.jar,/root/resource/fastjson-1.2.11.jar
 * ~/resource/AreaTop3RoadFlowAnalyze.jar
 * <p>
 * 这是一个分组取topN  SparkSQL分组取topN
 * 区域，道路流量排序         按照区域和道路进行分组
 */

public class AreaTop3RoadFlowAnalyze {

    // -Xms800m -Xmx800m  -XX:PermSize=64M -XX:MaxNewSize=256m -XX:MaxPermSize=128m
    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("areaTop3RoadFlow");

        /**
         * "SELECT * FROM table1 join table2 ON (连接条件)"  如果某一个表小于20M 他会自动广播出去
         * 会将小于spark.sql.autoBroadcastJoinThreshold值（默认为10M）的表广播到executor节点，不走shuffle过程,更加高效。
         */
//				conf.setConf("spark.sql.autoBroadcastJoinThreshold", "20971520");  //单位：B



        SparkSession sparkSession ;

        if(ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)){

            //本地运行
            conf.setMaster("local");

            sparkSession = SparkSession.builder().config(conf).getOrCreate();
            // 模拟数据
            SparkUtils.mockData(sparkSession);
        }else{
            //集群模式，开启hive支持.


            sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

            sparkSession.sql("use traffic");
        }

        // 注册自定义函数
        sparkSession.udf().register("concat_String_string", new ConcatStringStringUDF(), DataTypes.StringType);
        sparkSession.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
        sparkSession.udf().register("remove_random_prefix", new RemoveRandomPrefixUDF(), DataTypes.StringType);

        sparkSession.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());


        Long taskId = TrafficContext.getTaskId(args, Constants.SPARK_LOCAL_TASKID_TOPN_MONITOR_FLOW);
        if (taskId==null){
            System.out.println("taskId is null , exit....");
            return;
        }

        JSONObject taskParams = TrafficContext.getTaskParams(taskId);

        if(taskParams==null){
            System.out.println("taskParams is null , exit....");
            return;
        }

        /**
         * 获取指定日期内的参数
         * (areaId,row)
         * row: monitor_id,car,road_id,area_id
         */
        JavaPairRDD<String, Row> areaId2DetailInfos = getInfosByDateRDD(sparkSession, taskParams);

        /**
         * 从异构数据源MySQL中获取区域信息
         * (areaId,row)
         *
         * row : area_id	area_name
         * areaId2AreaInfoRDD<area_id,row:详细信息>
         */
        JavaPairRDD<String, Row> areaId2AreaInfoRDD = getAreaId2AreaInfoRDD(sparkSession);

        /**
         * 补全区域信息    添加区域名称
         * 	monitor_id car road_id	area_id	area_name
         * 生成基础临时信息表
         * 	tmp_car_flow_basic
         *
         * 将符合条件的所有的数据得到对应的中文区域名称 ，然后动态创建Schema的方式，将这些数据注册成临时表tmp_car_flow_basic
         */
        generateTempRoadFlowBasicTable(sparkSession, areaId2DetailInfos, areaId2AreaInfoRDD);

        /**
         * 统计各个区域各个路段车流量的临时表
         *
         * monitor_id car road_id	area_id	area_name
         *
         * area_name  road_id    car_count      monitor_infos
         *   黄埔区		  01		 100	  0001=20|0002=30|0003=50
         *   黄埔区		  02		 200	  0004=40|0005=60|0006=100
         *
         * 注册成临时表tmp_area_road_flow_count
         */

        generateTempAreaRoadFlowTable(sparkSession);

        /**
         *  area_name
         *	 road_id
         *	 count(*) car_count
         *	 monitor_infos
         * 使用开窗函数  获取每一个区域的topN路段
         */
        getAreaTop3RoadFolwRDD(sparkSession);

        System.out.println("***********ALL FINISHED*************");
        sparkSession.stop();

    }

    @SuppressWarnings("unused")
    private static void  getAreaTop3RoadFolwRDD(SparkSession sparkSession) {
        /**
         * tmp_area_road_flow_count表：
         * 		area_name
         * 		road_id
         * 		car_count
         * 		monitor_infos
         *
         * 	 area_name  road_id    car_count      monitor_infos
         *   黄埔区		  01		 100	  0001=20|0002=30|0003=50
         *   黄埔区		  02		 200	  0004=40|0005=60|0006=100
         */
        String sql = ""
                + "SELECT "
                    + "area_name,"
                    + "road_id,"
                    + "car_count,"
                    + "monitor_infos, "
                    + "CASE "
                        + "WHEN car_count > 170 THEN 'A LEVEL' "
                        + "WHEN car_count > 160 AND car_count <= 170 THEN 'B LEVEL' "
                        + "WHEN car_count > 150 AND car_count <= 160 THEN 'C LEVEL' "
                        + "ELSE 'D LEVEL' "
                    + "END flow_level "
                + "FROM ("
                        + "SELECT "
                        + "area_name,"
                        + "road_id,"
                        + "car_count,"
                        + "monitor_infos,"
                        + "row_number() OVER (PARTITION BY area_name ORDER BY car_count DESC) rn "
                        + "FROM tmp_area_road_flow_count "
                    + ") tmp "
                + "WHERE rn <=3";

        Dataset<Row> df = sparkSession.sql(sql);

        System.out.println("--------最终的结果-------");

        df.show();


    }

    private static void generateTempAreaRoadFlowTable(SparkSession sparkSession) {
        /**
         * 	structFields.add(DataTypes.createStructField("area_id", DataTypes.StringType, true));
         *	structFields.add(DataTypes.createStructField("area_name", DataTypes.StringType, true));
         *	structFields.add(DataTypes.createStructField("road_id", DataTypes.StringType, true));
         *	structFields.add(DataTypes.createStructField("monitor_id", DataTypes.StringType, true));
         *	structFields.add(DataTypes.createStructField("car", DataTypes.StringType, true));
         */
        /**
         *  tmp_car_flow_basic 临时表
         *
         *  area_id area_name car road_id monitor_id
         *  01        松江区   沪.... 01    0001
         *  01        松江区          01    0002
         *  01        松江区          01    0002
         *
         *
         *
         *  要求的结果
         *  * area_name  road_id    car_count      monitor_infos
         *   黄埔区		  01		 100	  0001=20|0002=30|0003=50
         *   黄埔区		  02		 200	  0004=40|0005=60|0006=100
         */
        String sql =
                "SELECT "
                        + "area_name,"
                        + "road_id,"
                        + "count(*) car_count,"
                        //group_concat_distinct 统计每一条道路中每一个卡扣下的车流量
                        + "group_concat_distinct(monitor_id) monitor_infos "
                        + "FROM tmp_car_flow_basic "
                        + "GROUP BY area_name,road_id";


        /**
         * 下面是当遇到区域下某个道路车辆特别多的时候，会有数据倾斜，怎么处理？random
         *  car_count  area_name_road_id   monitor_infos
         *    40         黄埔区:49          001=15|002=15|003=10
         *    60         黄埔区:49          001=25|002=15|003=20
         *
         *    相加得到：
         *    100        黄埔区:49          001=40|002=30|003=30
         */
        String sqlTest = ""
                + "SELECT "
                    + "area_name_road_id,"
                    + "sum(car_count),"
                    + "group_concat_distinct(monitor_infos) monitor_infoss "
                + "FROM ("
                    + "SELECT "
                    + "remove_random_prefix(area_name_road_id) area_name_road_id,"
                    + "car_count,"
                    + "monitor_infos "
                    + "FROM ("
                        + "SELECT "
                        + "area_name_road_id,"//1_黄浦区:49   2_黄浦区:49
                        + "count(*) car_count,"
                        + "group_concat_distinct(monitor_id) monitor_infos "
                        + "FROM ("
                            + "SELECT "
                            + "monitor_id,"
                            + "car,"
                            + "random_prefix(concat_String_string(area_name,road_id,':'),10) area_name_road_id "
                            + "FROM tmp_car_flow_basic "
                        + ") t1 "
                        + "GROUP BY area_name_road_id "
                    + ") t2 "
                + ") t3 "
                + "GROUP BY area_name_road_id";
        /**
         *  tmp_car_flow_basic 临时表
         *
         *  area_id area_name car road_id monitor_id
         *  01        松江区   沪.... 01    0001
         *  01        松江区          01    0002
         *  01        松江区          01    0002
        */
        //  area_name_road_id   carcount    monitor_infos
        // 黄埔区:49             500           0001=50|0002=100...
        // 黄埔区:49             600           0001=100|0002=80...
        // 松江区:50             400
        // 松江区:50             230

        Dataset<Row> df = sparkSession.sql(sql);

//		df.show();
        df.registerTempTable("tmp_area_road_flow_count");
    }

    /**
     * 获取符合条件数据对应的区域名称，并将这些信息注册成临时表 tmp_car_flow_basic
     *
     * @param sparkSession
     * @param areaId2DetailInfos
     * @param areaId2AreaInfoRDD
     */
    private static void  generateTempRoadFlowBasicTable(SparkSession sparkSession,
                                                       JavaPairRDD<String, Row> areaId2DetailInfos, JavaPairRDD<String, Row> areaId2AreaInfoRDD) {
        //
        JavaRDD<Row> tmpRowRDD = areaId2DetailInfos.join(areaId2AreaInfoRDD).map(
                new Function<Tuple2<String, Tuple2<Row, Row>>, Row>() {
                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Row call(Tuple2<String, Tuple2<Row, Row>> tuple) throws Exception {
                        String areaId = tuple._1;
                        Row carFlowDetailRow = tuple._2._1;
                        Row areaDetailRow = tuple._2._2;

                        String monitorId = carFlowDetailRow.getString(0);
                        String car = carFlowDetailRow.getString(1);
                        String roadId = carFlowDetailRow.getString(2);

                        String areaName = areaDetailRow.getString(1);

                        return RowFactory.create(areaId, areaName, roadId, monitorId, car);
                    }
                });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("area_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("road_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("monitor_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("car", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(structFields);

        Dataset<Row> df = sparkSession.createDataFrame(tmpRowRDD, schema);

        df.registerTempTable("tmp_car_flow_basic");

    }

    private static JavaPairRDD<String, Row> getAreaId2AreaInfoRDD(SparkSession sparkSession) {
        String url ;
        String user ;
        String password ;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        //获取Mysql数据库的url,user,password信息
        if (local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        Map<String, String> options = new HashMap<>();
        options.put("url", url);
        options.put("user", user);
        options.put("password", password);
        options.put("dbtable", "area_info");

        // 通过SQLContext去从MySQL中查询数据
        Dataset<Row> areaInfoDF = sparkSession.read().format("jdbc").options(options).load();

        System.out.println("------------Mysql数据库中的表area_info数据为------------");

        areaInfoDF.show();
        // 返回RDD
        JavaRDD<Row> areaInfoRDD = areaInfoDF.javaRDD();

        JavaPairRDD<String, Row> areaid2areaInfoRDD = areaInfoRDD.mapToPair(
                new PairFunction<Row, String, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        String areaid = String.valueOf(row.get(0));
                        return new Tuple2<>(areaid, row);
                    }
                });

        return areaid2areaInfoRDD;
    }

    /**
     * 获取日期内的数据
     *
     * @param sparkSession
     * @param taskParam
     * @return (areaId, row)
     */
    private static JavaPairRDD<String, Row> getInfosByDateRDD(SparkSession sparkSession, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql = "SELECT "
                + "monitor_id,"
                + "car,"
                + "road_id,"
                + "area_id "
                + "FROM	monitor_flow_action "
                + "WHERE date >= '" + startDate + "'"
                + "AND date <= '" + endDate + "'";
        Dataset<Row> df = sparkSession.sql(sql);

        return df.javaRDD().mapToPair(new PairFunction<Row, String, Row>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                String areaId = row.getString(3);
                return new Tuple2<>(areaId, row);
            }
        });
    }
}

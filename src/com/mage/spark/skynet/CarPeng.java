package com.mage.spark.skynet;

import com.alibaba.fastjson.JSONObject;
import com.mage.spark.constant.Constants;
import com.mage.spark.context.TrafficContext;
import com.mage.spark.util.ParamUtils;
import com.mage.spark.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 碰撞分析.....
 */

public class CarPeng {

    public static void main(String[] args) {

        SparkSession sparkSession = TrafficContext.initContext("Peng");


        //获取任务id
        Long taskId = TrafficContext.getTaskId(args, Constants.SPARK_LOCAL_TASKID_MONITOR);

        if(taskId==null){
            System.out.println("task id is null ! exit...");
            return;
        }
        //通过任务id,到数据库里查询此次任务的参数
        JSONObject taskParams = TrafficContext.getTaskParams(taskId);

        if(taskParams==null){
            System.out.println("task params is null ! exit...");
            return;
        }


//////		/**
//////		 * 区域碰撞分析,直接打印显示出来。
//////		 * "01","02" 指的是两个区域
//////		 */
        CarPeng(sparkSession, taskParams, "01", "02");
//////
//////	 	/**
//////	 	 * 卡扣碰撞分析，直接打印结果
//////	 	 *
//////	 	 */
        areaCarPeng(sparkSession, taskParams);

    }

    /**
     * 卡扣碰撞分析
     * 假设数据如下：
     * area1卡扣:["0000", "0001", "0002", "0003"]
     * area2卡扣:["0004", "0005", "0006", "0007"]
     */
    private static void areaCarPeng(SparkSession sparkSession, JSONObject taskParamsJsonObject) {
        List<String> monitorIds1 = Arrays.asList("0000", "0001", "0002", "0003");
        List<String> monitorIds2 = Arrays.asList("0004", "0005", "0006", "0007");
        // 通过两堆卡扣号，分别取数据库（本地模拟的两张表）中查询数据
        JavaRDD<Row> areaRDD1 = getAreaRDDByMonitorIds(sparkSession, taskParamsJsonObject, monitorIds1);
        JavaRDD<Row> areaRDD2 = getAreaRDDByMonitorIds(sparkSession, taskParamsJsonObject, monitorIds2);

        JavaPairRDD<String, String> distinct1 = areaRDD1.mapToPair(new PairFunction<Row, String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(3) + "", row.getAs(3) + "");
            }
        }).distinct();


        JavaPairRDD<String, String> distinct2 = areaRDD2.mapToPair(new PairFunction<Row, String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(3) + "", row.getAs(3) + "");
            }
        }).distinct();

        distinct1.join(distinct2).foreach(new VoidFunction<Tuple2<String, Tuple2<String, String>>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<String, String>> tuple)
                    throws Exception {
                System.out.println("车辆 ： "+tuple._1+"\n"
					+"在area1中经过的卡扣："+tuple._2._1+"\n"
					+"在area2中经过的卡扣："+tuple._2._2+"\n"
					+"********************"+"\n");

            }
        });

    }

    /**
     * 区域碰撞分析：两个区域共同出现的车辆
     *
     * @param area1
     * @param area2
     */
    private static void CarPeng(SparkSession sparkSession, JSONObject taskParamsJsonObject, String area1, String area2) {
        //得到01区域的数据放入rdd01
        JavaRDD<Row> cameraRDD01 = SparkUtils.getCameraRDDByDateRangeAndArea(sparkSession, taskParamsJsonObject, area1);

        //得到02区域的数据放入rdd02
        JavaRDD<Row> cameraRDD02 = SparkUtils.getCameraRDDByDateRangeAndArea(sparkSession, taskParamsJsonObject, area2);


        /**********不用distinct达到去重的目的*************/
        //去重后   (car,car)
        JavaPairRDD<String, String> m01 = cameraRDD01.mapToPair(new PairFunction<Row, String, Row>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                //row.getString(3) ----- car
                return new Tuple2<>(row.getString(3), row);
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> arg0)
                    throws Exception {
                return new Tuple2<>(arg0._1, arg0._1);
            }
        });

        JavaPairRDD<String, String> m02 = cameraRDD02.mapToPair(new PairFunction<Row, String, Row>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(3), row);
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(
                    Tuple2<String, Iterable<Row>> arg0)
                    throws Exception {
                return new Tuple2<>(arg0._1, arg0._1);
            }
        });

        //rdd01和rdd02 join 打印同时出现的车辆
        m01.join(m02).foreach(new VoidFunction<Tuple2<String, Tuple2<String, String>>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<String, String>> arg0)
                    throws Exception {
                System.out.println("同时出现在两个区域的车辆有----" + arg0._1);

            }
        });
    }


    private static JavaRDD<Row> getAreaRDDByMonitorIds(SparkSession sparkSession, JSONObject taskParamsJsonObject, List<String> monitorId1) {
        String startTime = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_START_DATE);
        String endTime = ParamUtils.getParam(taskParamsJsonObject, Constants.PARAM_END_DATE);
        String sql = "SELECT * "
                + "FROM monitor_flow_action"
                + " WHERE date >='" + startTime + "' "
                + " AND date <= '" + endTime + "' "
                + " AND monitor_id in (";

        for (int i = 0; i < monitorId1.size(); i++) {
            sql += "'" + monitorId1.get(i) + "'";

            if (i < monitorId1.size() - 1) {
                sql += ",";
            }
        }

        sql += ")";

        return sparkSession.sql(sql).javaRDD();
    }

}

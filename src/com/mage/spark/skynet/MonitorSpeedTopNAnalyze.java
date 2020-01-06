package com.mage.spark.skynet;

import com.alibaba.fastjson.JSONObject;
import com.mage.spark.constant.Constants;
import com.mage.spark.context.TrafficContext;
import com.mage.spark.dao.IMonitorDAO;
import com.mage.spark.dao.factory.DAOFactory;
import com.mage.spark.domain.TopNMonitorDetailInfo;
import com.mage.spark.util.SparkUtils;
import com.mage.spark.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 高速卡口TopN..
 */
public class MonitorSpeedTopNAnalyze {

    public static void main(String[] args) {

        SparkSession sparkSession = TrafficContext.initContext("MoinitorSpeedTop");

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

        /**
         * 通过params（json字符串）查询monitor_flow_action
         *
         * 获取指定日期内检测的monitor_flow_action中车流量数据，返回JavaRDD<Row>
         */
        JavaRDD<Row> cameraRDD = SparkUtils.getCameraRDDByDateRange(sparkSession, taskParams);

        /**
         * 将row类型的RDD 转换成kv格式的RDD   k:monitor_id  v:row
         */
        JavaPairRDD<String, Row> monitor2DetailRDD = SparkUtils.getMonitor2DetailRDD(cameraRDD);


        /**
         * 按照卡扣号分组，对应的数据是：每个卡扣号(monitor)对应的Row信息
         * 由于一共有9个卡扣号，这里groupByKey后一共有9组数据。
         */
        JavaPairRDD<String, Iterable<Row>> monitorId2RowsRDD = monitor2DetailRDD.groupByKey();

        /**
         * 获取高速通过的TOPN卡扣
         */
        List<String> top5MonitorIds = speedTopNMonitor(monitorId2RowsRDD);
        for (String monitorId : top5MonitorIds) {
            System.out.println("车辆经常高速通过的卡扣	monitorId:" + monitorId);
        }

        /**
         * 获取车辆高速通过的TOPN卡扣，每一个卡扣中车辆速度最快的前10名，并存入数据库表 top10_speed_detail 中
         */
        saveMonitorDetails(sparkSession, taskId, top5MonitorIds, monitor2DetailRDD);
    }



    /**
     * 获取经常高速通过的TOPN卡扣 , 返回车辆经常高速通过的卡扣List
     * <p>
     * 1、每一辆车都有speed    按照速度划分是否是高速 中速 普通 低速
     * 2、每一辆车的车速都在一个车速段     对每一个卡扣进行聚合   拿到高速通过 中速通过  普通  低速通过的车辆各是多少辆
     * 3、四次排序   先按照高速通过车辆数   中速通过车辆数   普通通过车辆数   低速通过车辆数
     *
     * @param groupByMonitorId ---- (monitorId ,Iterable[Row])
     * @return List<MonitorId> 返回车辆经常高速通过的卡扣List
     */
    private static List<String> speedTopNMonitor(JavaPairRDD<String, Iterable<Row>> groupByMonitorId) {
        /**
         * key:自定义的类  value：卡扣ID
         */
        JavaPairRDD<SpeedSortKey, String> speedSortKey2MonitorId =
                groupByMonitorId.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, SpeedSortKey, String>() {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<SpeedSortKey, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String monitorId = tuple._1;
                        Iterator<Row> speedIterator = tuple._2.iterator();

                        /**
                         * 这四个遍历 来统计这个卡扣下 高速 中速 正常 以及低速通过的车辆数
                         */
                        long lowSpeed = 0;
                        long normalSpeed = 0;
                        long mediumSpeed = 0;
                        long highSpeed = 0;

                        while (speedIterator.hasNext()) {
                            int speed = StringUtils.convertStringtoInt(speedIterator.next().getString(5));
                            if (speed >= 0 && speed < 60) {
                                lowSpeed++;
                            } else if (speed >= 60 && speed < 90) {
                                normalSpeed++;
                            } else if (speed >= 90 && speed < 120) {
                                mediumSpeed++;
                            } else if (speed >= 120) {
                                highSpeed++;
                            }
                        }
                        SpeedSortKey speedSortKey = new SpeedSortKey(lowSpeed, normalSpeed, mediumSpeed, highSpeed);
                        return new Tuple2<>(speedSortKey, monitorId);
                    }
                });
        /**
         * key:自定义的类  value：卡扣ID
         */

        JavaPairRDD<SpeedSortKey, String> sortBySpeedCount = speedSortKey2MonitorId.sortByKey(false);


        /**
         * 硬编码问题
         * 取出前5个经常速度高的卡扣
         */
        //int topNumFromParams = Integer.parseInt(ParamUtils.getParam(taskParamsJsonObject, Constants.FIELD_TOP_NUM));
        List<Tuple2<SpeedSortKey, String>> take = sortBySpeedCount.take(5);

        List<String> monitorIds = new ArrayList<>();
        for (Tuple2<SpeedSortKey, String> tuple : take) {
            monitorIds.add(tuple._2);
        }
        return monitorIds;
    }

    /**
     * 获取车辆经常高速通过的TOPN卡扣，每一个卡扣中车辆速度最快的前10名，并存入数据库表 top10_speed_detail 中
     *
     * @param taskId
     * @param
     * @param monitor2DetailRDD
     */
    private static void saveMonitorDetails(SparkSession sparkSession, final long taskId, List<String> top5MonitorIds, JavaPairRDD<String, Row> monitor2DetailRDD) {

        /**
         * top5MonitorIds这个集合里面都是monitor_id
         */
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        final Broadcast<List<String>> top5MonitorIdsBroadcast = sc.broadcast(top5MonitorIds);

        /**
         * 我们想获取每一个卡扣的详细信息，就是从monitor2DetailRDD中取出来包含在top10MonitorIds集合的卡扣的信息
         */
        monitor2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                String monitorId = tuple._1;
                List<String> list = top5MonitorIdsBroadcast.value();
                return list.contains(monitorId);
            }

        }).groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Row>>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                Iterator<Row> rowsIterator = tuple._2.iterator();

                Row[] top10Cars = new Row[10];
                while (rowsIterator.hasNext()) {
                    Row row = rowsIterator.next();

                    long speed = Long.valueOf(row.getString(5));

                    for (int i = 0; i < top10Cars.length; i++) {
                        if (top10Cars[i] == null) {
                            top10Cars[i] = row;
                            break;
                        } else {
                            long _speed = Long.valueOf(top10Cars[i].getString(5));
                            if (speed > _speed) {
                                for (int j = 9; j > i; j--) {
                                    top10Cars[j] = top10Cars[j - 1];
                                }
                                top10Cars[i] = row;
                                break;
                            }
                        }
                    }
                }

//                另外一种排序算法。
//                List list = IteratorUtils.toList(rowsIterator);
//
//                Collections.sort(list, new Comparator() {
//                    @Override
//                    public int compare(Object o1, Object o2) {
//                        Row row1 = (Row)o1;
//                        Row row2 = (Row)o2;
//                        long speed1 = Long.valueOf(row1.getString(5));
//                        long speed2 = Long.valueOf(row2.getString(5));
//                        if(speed2==speed1){
//                            return 0;
//                        }else {
//                            return (int)(speed2 - speed1);//降序
//                        }
//
//                    }
//                });

//                Object[] top10Cars = list.subList(0, 10).toArray();

                /**
                 * 将车辆通过速度最快的前N个卡扣中每个卡扣通过的车辆的速度最快的前10名存入数据库表 top10_speed_detail中
                 */
                IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                List<TopNMonitorDetailInfo> topNMonitorDetailInfos = new ArrayList<>();
                for (Row row : top10Cars) {
                    topNMonitorDetailInfos.add(new TopNMonitorDetailInfo(taskId,
                            row.getString(0), row.getString(1), row.getString(2),
                            row.getString(3), row.getString(4), row.getString(5),
                            row.getString(6)));
                }

                monitorDAO.insertBatchTop10Details(topNMonitorDetailInfos);
            }
        });
    }
}

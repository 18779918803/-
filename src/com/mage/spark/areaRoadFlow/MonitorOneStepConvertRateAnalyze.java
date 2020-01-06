package com.mage.spark.areaRoadFlow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.mage.spark.constant.Constants;
import com.mage.spark.context.TrafficContext;
import com.mage.spark.dao.ITaskDAO;
import com.mage.spark.dao.factory.DAOFactory;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;
import com.mage.spark.domain.Task;
import com.mage.spark.util.DateUtils;
import com.mage.spark.util.NumberUtils;
import com.mage.spark.util.ParamUtils;
import com.mage.spark.util.SparkUtils;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * 卡口转化率...
 *
 * monitor_id   1 2 3 4      1_2 2_3 3_4
 * 指定一个道路流  1 2 3 4
 * 1 carCount1 2carCount2  转化率 carCount2/carCount1
 * 1 2 3  转化率                           1 2 3的车流量/1 2的车流量
 * 1 2 3 4 转化率       1 2 3 4的车流量 / 1 2 3 的车流量
 * 沪A1234	1,2,3,6,2,3
 * 1、查询出来的数据封装到cameraRDD
 * 2、计算每一车的轨迹
 * 3、匹配指定的道路流       1：carCount   1，2：carCount   1,2,3carCount
 *
 * @author root
 */

public class MonitorOneStepConvertRateAnalyze {
    public static void main(String[] args) {
        /**
         * 初始化上下文环境，分为集运运行方式和本地运行方式两种
         */
        SparkSession sparkSession = TrafficContext.initContext("MonitorOneStepConvertRate");

        //获取任务id
        Long taskId = TrafficContext.getTaskId(args, Constants.SPARK_LOCAL_TASKID_MONITOR_ONE_STEP_CONVERT);

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
         * 从数据库中查找出来我们指定的卡扣流
         * 0001,0002,0003,0004,0005
         */
        String roadFlow = ParamUtils.getParam(taskParams, Constants.PARAM_MONITOR_FLOW);

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        final Broadcast<String> roadFlowBroadcast = sc.broadcast(roadFlow);

        /**
         * 通过时间的范围拿到合法的车辆
         */
        JavaRDD<Row> rowRDDByDateRange = SparkUtils.getCameraRDDByDateRange(sparkSession, taskParams);
        /**
         * 将rowRDDByDateRange 变成key-value对的形式，key car value 详细信息
         * （key,row）
         * 为什么要变成k v对的形式？
         * 因为下面要对car 按照时间排序，绘制出这辆车的轨迹。
         */
        JavaPairRDD<String, Row> car2RowRDD =  getCar2RowRDD(rowRDDByDateRange);


        /**
         * 计算这一辆车，有多少次匹配到我们指定的卡扣流
         *
         * 先拿到车辆的轨迹，比如一辆车轨迹：0001,0002,0003,0004,0001,0002,0003,0001,0004
         * 返回一个二元组（切分的片段，该片段对应的该车辆轨迹中匹配上的次数）
         * ("0001",3)
         * ("0001,0002",2)
         * ("0001,0002,0003",2)
         * ("0001,0002,0003,0004",1)
         * ("0001,0002,0003,0004,0005",0)
         * ... ...
         * ("0001",13)
         * ("0001,0002",12)
         * ("0001,0002,0003",11)
         * ("0001,0002,0003,0004",11)
         * ("0001,0002,0003,0004,0005",10)
         */
        JavaPairRDD<String, Long> roadSplitRDD = generateAndMatchRowSplit(taskParams, roadFlowBroadcast, car2RowRDD);

        /**
         * roadSplitRDD
         * 所有的相同的key先聚合得到总数
         * ("0001",500)
         * ("0001,0002",400)
         * ("0001,0002,0003",300)
         * ("0001,0002,0003,0004",200)
         * ("0001,0002,0003,0004,0005",100)
         * 变成了一个 K,V格式的map
         * ("0001",500)
         * ("0001,0002",400)
         * ("0001,0002,0003",300)
         * ("0001,0002,0003,0004",200)
         * ("0001,0002,0003,0004,0005",100)
         */
        Map<String, Long> roadFlow2Count = getRoadFlowCount(roadSplitRDD);

        Map<String, Double> convertRateMap = computeRoadSplitConvertRate(roadFlowBroadcast, roadFlow2Count);

        Set<Entry<String, Double>> entrySet = convertRateMap.entrySet();
        for (Entry<String, Double> entry : entrySet) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
    }

    private static Map<String, Long> getRoadFlowCount(JavaPairRDD<String, Long> roadSplitRDD) {

        JavaPairRDD<String, Long> sumByKey = roadSplitRDD.reduceByKey(new Function2<Long, Long, Long>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        /**
         * ("0001",113)
         *  ("0001,0002,0003,0004,0005",10)
         * ("0001,0002",110)
         * ("0001,0002,0003",12)
         * ("0001,0002,0003,0004",11)

         * 转换成Map出去
         */
        Map<String, Long> map = new HashMap<>();

        List<Tuple2<String, Long>> results = sumByKey.collect();

        for (Tuple2<String, Long> tuple : results) {
            map.put(tuple._1, tuple._2);
        }

        return map;
    }

    /**
     * @param roadFlowBroadcast
     * @param roadFlow2Count
     * @return Map<String, Double>
     */
    private static Map<String, Double> computeRoadSplitConvertRate(Broadcast<String> roadFlowBroadcast, Map<String, Long> roadFlow2Count) {
        //0001，0002，0003，0004，0005
        String roadFlow = roadFlowBroadcast.value();
        String[] split = roadFlow.split(",");

        /**
         * 存放卡扣切面的转换率
         * "0001,0002" 0.16
         */
        Map<String, Double> rateMap = new HashMap<>();
        long lastMonitorCarCount = 0L;
        String tmpRoadFlow = "";
        for (int i = 0; i < split.length; i++) {
            tmpRoadFlow += "," + split[i]; //,0001
            /**
             * roadFlow2Count
             * <001,300>
             *
             * <"001,002",200>
             *
             * <"001,002,003",100>
             */
            Long count = roadFlow2Count.get(tmpRoadFlow.substring(1));
            if (count != null) {
                /**
                 * 1_2
                 * lastMonitorCarCount      1 count
                 */
                if (i != 0 && lastMonitorCarCount != 0L) {
                    double rate = NumberUtils.formatDouble((double) count / (double) lastMonitorCarCount, 2);
                    rateMap.put(tmpRoadFlow.substring(1), rate);
                }
                lastMonitorCarCount = count;
            }
        }
        return rateMap;
    }

    /**
     * car2RowRDD car   row详细信息
     * 按照通过时间进行排序，拿到他的轨迹
     *
     * @param taskParam
     * @param roadFlowBroadcast ---- 0001,0002,0003,0004,0005
     * @param car2RowRDD
     * @return 二元组(切分的片段，该片段在本次车辆轨迹中出现的总数)
     */
    private static JavaPairRDD<String, Long> generateAndMatchRowSplit(JSONObject taskParam,
                                                                      final Broadcast<String> roadFlowBroadcast, JavaPairRDD<String, Row> car2RowRDD) {
        return car2RowRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Long>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<Tuple2<String, Long>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                Iterator<Row> iterator = tuple._2.iterator();
                List<Tuple2<String, Long>> list = new ArrayList<>();

                List<Row> rows = new ArrayList<>();
                /**
                 * 遍历的这一辆车的所有的详细信息，然后将详细信息放入到rows集合中
                 */
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    rows.add(row);
                }


                /**
                 * 对这个rows集合 按照车辆通过卡扣的时间排序
                 */
                Collections.sort(rows, new Comparator<Row>() {

                    @Override
                    public int compare(Row row1, Row row2) {
                        String actionTime1 = row1.getString(4);
                        String actionTime2 = row2.getString(4);
                        if (DateUtils.after(actionTime1, actionTime2)) {
                            return 1;
                        } else {
                            if (actionTime1.equals(actionTime2)) {
                                return 0;
                            }
                            return -1;
                        }
                    }
                });

                /**
                 * roadFlowBuilder保存到是本次车辆的轨迹是一组逗号分开的卡扣id，组合起来就是这辆车的运行轨迹
                 */
                StringBuilder roadFlowBuilder = new StringBuilder();

                /**
                 * roadFlowBuilder怎么拼起来的？  rows是由顺序了，直接遍历然后追加到roadFlowBuilder就可以了吧。
                 * row.getString(1) ---- monitor_id
                 */
                for (Row row : rows) {
                    roadFlowBuilder.append("," + row.getString(1));
                }
                /**
                 * roadFlowBuilder这里面的开头有一个逗号， 去掉逗号。
                 * roadFlow是本次车辆的轨迹
                 *
                 *
                 */
                String roadFlow = roadFlowBuilder.toString().substring(1);


                /**
                 *  从广播变量中获取指定的卡扣流参数
                 *  0001,0002,0003,0004,0005
                 *  001,
                 *  001,002
                 *  001,002,003
                 *  001,002,003,004
                 *  001,002,003,004,005
                 *
                 */
                String standardRoadFlow = roadFlowBroadcast.value();

                /**
                 * 对指定的卡扣流参数分割
                 */
                String[] split = standardRoadFlow.split(",");

                /**
                 *
                 * 1 2 3 4 5
                 * 遍历分割完成的数组
                 *
                 * 结果数据：(0001,3)
                 * ("0001,0002",2))
                 * ("0001,0002,0003",2))
                 * ("0001,0002,0003,0004",2))
                 * ("0001,0002,0003,0004,0005",1))
                 *
                 * split : [0001,0002,0003,0004,0005]
                 * roadFlow: 0003,0000,0000,0004,0001,0003,0002,0001,0002,0003,0005,0002,0007,0007,0000
                 */
                for (int i = 1; i <= split.length; i++) {
                    //临时组成的卡扣切片  1,2 1,2,3
                    String tmpRoadFlow = "";
                    /**
                     * 第一次进来：,0001
                     * 第二次进来：,0001,0002
                     */
                    for (int j = 0; j < i; j++) {
                        tmpRoadFlow += "," + split[j];//,0001
                    }

                    tmpRoadFlow = tmpRoadFlow.substring(1);//去掉前面的逗号 0001

                    //indexOf 从哪个位置开始查找
                    int index = 0;
                    //这辆车有多少次匹配到这个卡扣切片的次数
                    Long count = 0L;
                    //0001,0002,0003,0004      0001

                    while (roadFlow.indexOf(tmpRoadFlow, index) != -1) {
                        count++;
                        index = roadFlow.indexOf(tmpRoadFlow, index) + 1;
                    }
                    list.add(new Tuple2<>(tmpRoadFlow, count));
                }
                return list.iterator();
            }
        });
    }

    private static JavaPairRDD<String, Row> getCar2RowRDD(JavaRDD<Row> car2RowRDD) {
        return car2RowRDD.mapToPair(new PairFunction<Row, String, Row>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(3), row);
            }
        });
    }
}

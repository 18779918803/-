package com.mage.spark.skynet;

import java.util.*;

import com.mage.spark.context.TrafficContext;

import com.mage.spark.domain.TopNMonitor2CarCount;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;

import com.mage.spark.constant.Constants;

import com.mage.spark.dao.IMonitorDAO;
import com.mage.spark.dao.factory.DAOFactory;

import com.mage.spark.domain.MonitorState;

import com.mage.spark.domain.TopNMonitorDetailInfo;
import com.mage.spark.util.ParamUtils;
import com.mage.spark.util.SparkUtils;
import com.mage.spark.util.StringUtils;


/**
 * 卡扣流量监控模块
 * 1.检测卡扣状态
 * 2.获取车流排名前N的卡扣号
 * 3.数据库保存累加器5个状态（正常卡扣数，异常卡扣数，正常摄像头数，异常摄像头数，异常摄像头的详细信息）
 * 4.topN 卡口的车流量具体信息存库
 * <p>
 * <p>
 * ./spark-submit  --master spark://node1:7077,node2:7077
 * --class com.mage.spark.skynet.MonitorStateAnalyze
 * --driver-class-path ../lib/mysql-connector-java-5.1.6.jar:../lib/fastjson-1.2.11.jar
 * --jars ../lib/mysql-connector-java-5.1.6.jar,../lib/fastjson-1.2.11.jar
 * ../lib/ProduceData2Hive.jar
 * 1
 *
 * @author root
 */
public class MonitorStateAnalyze {

    public static void main(String[] args) {
        /**
         * 初始化上下文环境，分为集运运行方式和本地运行方式两种
         */
        SparkSession sparkSession = TrafficContext.initContext("MonitorStateAnalyze");

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
         * 创建了一个自定义的累加器
         */


        SparkContext sparkContext = sparkSession.sparkContext();

        MonitorStateAccumulator accumulator = new MonitorStateAccumulator();

        sparkContext.register(accumulator,"monitorState");


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
         * 持久化..
         */
        monitorId2RowsRDD = monitorId2RowsRDD.cache();

        /**
         * 遍历分组后的RDD，拼接字符串
         * 数据中一共就有9个monitorId信息，那么聚合之后的信息也是9条
         * monitor_id=|cameraIds=|area_id=|camera_count=|carCount=
         * 例如:
         * ("0005","monitorId=0005|areaId=02|camearIds=09200,03243,02435,03232|cameraCount=4|carCount=100")
         *
         */
        JavaPairRDD<String, String> aggregateMonitorId2DetailRDD = aggreagteByMonitor(monitorId2RowsRDD);

        /**
         * 检测卡扣状态
         * carCount2MonitorRDD
         * K:car_count V:monitor_id
         * RDD(卡扣对应车流量总数,对应的卡扣号)
         */
        JavaPairRDD<Integer, String> carCount2MonitorRDD =
                checkMonitorState(sparkSession, aggregateMonitorId2DetailRDD,  accumulator);

        /**
         * 获取车流排名前N的卡扣号
         * 并放入数据库表  topn_monitor_car_count 中
         * return  KV格式的RDD  K：monitor_id V:monitor_id
         * 返回的是topN的(monitor_id,monitor_id)
         */
        JavaPairRDD<String, String> topNMonitor2CarFlow =
                getTopNMonitorCarFlow(sparkSession, taskId, taskParams, carCount2MonitorRDD);

        /**
         * 往数据库表  monitor_state 中保存 累加器累加的五个状态
         */
        saveMonitorState(taskId, accumulator);


        /**
         * 获取topN 卡口的车流量具体信息，存入数据库表 topn_monitor_detail_info 中
         */
        saveTopNDetails(taskId, topNMonitor2CarFlow, monitor2DetailRDD);


        System.out.println("******All is finished*******");
        sparkSession.stop();
    }


    /**
     * 按照monitor_id进行聚合
     *
     * @return ("monitorId","monitorId=xxx|areaId=xxx|cameraIds=xxx|cameraCount=xxx|carCount=xxx")
     * ("0005","monitorId=0005|areaId=02|camearIds=09200,03243,02435,03232|cameraCount=4|carCount=100")
     * 假设其中一条数据是以上这条数据，那么说明在这个0005卡扣下有4个camera,那么这个卡扣一共通过了100辆车信息.
     */
    private static JavaPairRDD<String, String> aggreagteByMonitor(JavaPairRDD<String, Iterable<Row>> monitorId2RowRDD) {
        /**
         * <monitor_id,List<Row> 集合里面的一个row记录代表的是camera的信息，row也可以说是代表的一辆车的信息。>
         */


        /**
         * 一个monitor_id对应一条记录
         * 为什么使用mapToPair来遍历数据，因为我们要操作的返回值是每一个monitorid 所对应的详细信息
         */
        JavaPairRDD<String, String> monitorId2CameraCountRDD = monitorId2RowRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, String, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                String monitorId = tuple._1;
                Iterator<Row> rowIterator = tuple._2.iterator();

                List<String> list = new ArrayList<>();//同一个monitorId下，对应的所有的不同的cameraId,list.count方便知道此monitor下对应多少个cameraId

                StringBuilder tmpInfos = new StringBuilder();//同一个monitorId下，对应的所有的不同的camearId信息

                int count = 0;//统计车辆数的count
                String areaId = "";
                /**
                 * 这个while循环  代表的是当前的这个卡扣一共经过了多少辆车，   一辆车的信息就是一个row
                 */
                while (rowIterator.hasNext()) {
                    //2018-06-27	0007	00536	京R66884	2018-06-27 11:30:25	30	41	08
                    Row row = rowIterator.next();
                    areaId = row.getString(7);
                    String cameraId = row.getString(2);
                    if (!list.contains(cameraId)) {
                        list.add(cameraId);
                    }
                    //针对同一个卡扣 monitor，append不同的cameraId信息 ,002,003,004
                    if (!tmpInfos.toString().contains(cameraId)) {
                        tmpInfos.append("," + row.getString(2));
                    }
                    //这里的count就代表的车辆数，一个row一辆车
                    count++;
                }

                /**
                 * camera_count
                 */
                int cameraCount = list.size();

                String infos = Constants.FIELD_MONITOR_ID + "=" + monitorId + "|"
                        + Constants.FIELD_AREA_ID + "=" + areaId + "|"
                        + Constants.FIELD_CAMERA_IDS + "=" + tmpInfos.toString().substring(1) + "|"
                        + Constants.FIELD_CAMERA_COUNT + "=" + cameraCount + "|"
                        + Constants.FIELD_CAR_COUNT + "=" + count;
                //("0005","monitorId=0005|areaId=02|camearIds=09200,03243,02435,03232|cameraCount=4|carCount=100")
                return new Tuple2<>(monitorId, infos);
            }
        });
        //<monitor_id,camera_infos(ids,cameracount,carCount)>
        return monitorId2CameraCountRDD;
    }

    /**
     * 往数据库中保存 累加器累加的五个状态
     *
     * @param taskId
     * @param
     */
    private static void saveMonitorState(Long taskId, AccumulatorV2<String,String> accumulator) {
        /**
         * 累加器中值能在Executor段读取吗？
         * 		不能
         * 这里的读取时在Driver中进行的
         */

        //abnormalMonitorCount=2|abnormalCameraCount=100|abnormalMonitorCameraInfos="0002":07553,07554~0004:8979,7987
        String accumulatorVal = accumulator.value();
        String normalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_MONITOR_COUNT);
        String normalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_NORMAL_CAMERA_COUNT);
        String abnormalMonitorCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_COUNT);
        String abnormalCameraCount = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_CAMERA_COUNT);
        String abnormalMonitorCameraInfos = StringUtils.getFieldFromConcatString(accumulatorVal, "\\|", Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS);

        /**
         * 这里面只有一条记录
         */
        MonitorState monitorState = new MonitorState(taskId, normalMonitorCount, normalCameraCount, abnormalMonitorCount, abnormalCameraCount, abnormalMonitorCameraInfos);

        /**
         * 向数据库表monitor_state中添加累加器累计的各个值
         */
        IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
        monitorDAO.insertMonitorState(monitorState);
    }


    /**
     * 获取topN 卡口的车流量具体信息，存入数据库表 topn_monitor_detail_info 中
     *
     * @param taskId
     * @param topNMonitor2CarFlow ---- (monitorId,monitorId)
     * @param monitor2DetailRDD   ---- (monitorId,Row)
     */
    private static void saveTopNDetails(
            final long taskId, JavaPairRDD<String, String> topNMonitor2CarFlow,
            final JavaPairRDD<String, Row> monitor2DetailRDD) {
        /**
         * 获取车流量排名前N的卡口的详细信息   可以看一下是在什么时间段内卡口流量暴增的。
         */

        /**
         * 优化点：
         * 因为topNMonitor2CarFlow 里面有只有5条数据，可以将这五条数据封装到广播变量中，然后遍历monitor2DetailRDD ，每遍历一条数据与广播变量中的值作比对。
         */
        topNMonitor2CarFlow.join(monitor2DetailRDD).mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> t) throws Exception {
                        return new Tuple2<>(t._1, t._2._2);
                    }
                }).foreachPartition(new VoidFunction<Iterator<Tuple2<String, Row>>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Tuple2<String, Row>> t) throws Exception {

                List<TopNMonitorDetailInfo> monitorDetailInfos = new ArrayList<>();


                while (t.hasNext()) {

                    Tuple2<String, Row> tuple = t.next();
                    Row row = tuple._2;
                    TopNMonitorDetailInfo m = new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6));
                    monitorDetailInfos.add(m);

                    if (monitorDetailInfos.size() >= 500) {
                        IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                        monitorDAO.insertBatchMonitorDetails(monitorDetailInfos);
                        monitorDetailInfos.clear();
                    }


                }

                /**
                 * 将topN的卡扣车流量明细数据 存入topn_monitor_detail_info 表中
                 */
                IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                monitorDAO.insertBatchMonitorDetails(monitorDetailInfos);
            }
        });

        /********************************使用广播变量来实现************************************/
        //将topNMonitor2CarFlow（只有5条数据）转成非K,V格式的数据，便于广播出去
        JavaRDD<String> topNMonitorCarFlow = topNMonitor2CarFlow.map(new Function<Tuple2<String, String>, String>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Tuple2<String, String> tuple) throws Exception {
                return tuple._1;
            }
        });

//        List<Tuple2<String, String>> collect = topNMonitor2CarFlow.collect();

        List<String> topNMonitorIds = topNMonitorCarFlow.collect();
        JavaSparkContext jsc = new JavaSparkContext(topNMonitor2CarFlow.context());
        final Broadcast<List<String>> broadcast_topNMonitorIds = jsc.broadcast(topNMonitorIds);
        JavaPairRDD<String, Row> filterTopNMonitor2CarFlow = monitor2DetailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;


            public Boolean call(Tuple2<String, Row> monitorTuple) throws Exception {

                return broadcast_topNMonitorIds.value().contains(monitorTuple._1);
            }
        });
        filterTopNMonitor2CarFlow.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Row>>>() {

            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Iterator<Tuple2<String, Row>> t)
                    throws Exception {
                List<TopNMonitorDetailInfo> monitorDetailInfos = new ArrayList<>();
                while (t.hasNext()) {
                    Tuple2<String, Row> tuple = t.next();
                    Row row = tuple._2;
                    TopNMonitorDetailInfo m = new TopNMonitorDetailInfo(taskId, row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4), row.getString(5), row.getString(6));
                    monitorDetailInfos.add(m);
                }
                /**
                 * 如果日后数据量变大了，list放不下了
                 * 将topN的卡扣车流量明细数据 存入topn_monitor_detail_info 表中
                 */
                IMonitorDAO monitorDAO = DAOFactory.getMonitorDAO();
                monitorDAO.insertBatchMonitorDetails(monitorDetailInfos);
            }
        });
    }

    /**
     * 获取卡口流量的前N名，并且持久化到数据库中
     * N是在数据库条件中取值
     *
     * @param taskId
     * @param taskParamsJsonObject
     * @param carCount2MonitorId----RDD(卡扣对应的车流量总数,对应的卡扣号)
     */
    private static JavaPairRDD<String, String> getTopNMonitorCarFlow(
            SparkSession sparkSession,
            long taskId, JSONObject taskParamsJsonObject,
            JavaPairRDD<Integer, String> carCount2MonitorId) {
        /**
         * 获取车流量排名前N的卡口信息
         * 有什么作用？ 当某一个卡口的流量这几天突然暴增和往常的流量不相符，交管部门应该找一下原因，是什么问题导致的，应该到现场去疏导车辆。
         */
        int topNumFromParams = Integer.parseInt(ParamUtils.getParam(taskParamsJsonObject, Constants.FIELD_TOP_NUM));

        /**
         * carCount2MonitorId <carCount,monitor_id>
         */
        List<Tuple2<Integer, String>> topNCarCount = carCount2MonitorId.sortByKey(false).take(topNumFromParams);

        //封装到对象中
        List<TopNMonitor2CarCount> topNMonitor2CarCounts = new ArrayList<>();
        for (Tuple2<Integer, String> tuple : topNCarCount) {
            TopNMonitor2CarCount topNMonitor2CarCount = new TopNMonitor2CarCount(taskId, tuple._2, tuple._1);
            topNMonitor2CarCounts.add(topNMonitor2CarCount);
        }

        /**
         * 得到DAO 将数据插入数据库
         * 向数据库表 topn_monitor_car_count 中插入车流量最多的TopN数据
         */
        IMonitorDAO ITopNMonitor2CarCountDAO = DAOFactory.getMonitorDAO();
        ITopNMonitor2CarCountDAO.insertBatchTopN(topNMonitor2CarCounts);

        /**
         * monitorId2MonitorIdRDD ---- K:monitor_id V:monitor_id
         * 获取topN卡口的详细信息
         * monitorId2MonitorIdRDD.join(monitorId2RowRDD)
         */
        List<Tuple2<String, String>> monitorId2CarCounts = new ArrayList<>();

        for (Tuple2<Integer, String> t : topNCarCount) {
            monitorId2CarCounts.add(new Tuple2<>(t._2, t._2));
        }

        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        JavaPairRDD<String, String> monitorId2MonitorIdRDD = sc.parallelizePairs(monitorId2CarCounts);

        return monitorId2MonitorIdRDD;
    }


    /**
     * 检测卡口状态
     *
     * @return RDD(实际卡扣对应车流量总数, 对应的卡扣号)
     */
    private static JavaPairRDD<Integer, String> checkMonitorState(
            SparkSession sparkSession,
            JavaPairRDD<String, String> monitorId2CameraCountRDD,
            final AccumulatorV2 accumulator) {
        /**
         * 从monitor_camera_info标准表中查询出来每一个卡口对应的camera的数量
         */
        String sqlText = "SELECT * FROM monitor_camera_info";
        Dataset<Row> standardDF = sparkSession.sql(sqlText);

        JavaRDD<Row> standardRDD = standardDF.javaRDD();
        /**
         * 使用mapToPair算子将standardRDD变成KV格式的RDD
         * monitorId2CameraId   :
         * (K:monitor_id  v:camera_id)
         */
        JavaPairRDD<String, String> monitorId2CameraId = standardRDD.mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row.getString(1));
            }
        });

        /**
         * 对每一个卡扣下面的信息进行统计，统计出来camera_count（这个卡扣下一共有多少个摄像头）,camera_ids(这个卡扣下，所有的摄像头编号拼接成的字符串)
         * 返回：
         * 	("monitorId","cameraIds=xxx|cameraCount=xxx")
         * 例如：
         * 	("0008","cameraIds=02322,01213,03442|cameraCount=3")
         * 如何来统计？
         * 	1、按照monitor_id分组
         * 	2、使用mapToPair遍历，遍历的过程可以统计
         */
        JavaPairRDD<String, String> standardMonitor2CameraInfos = monitorId2CameraId.groupByKey()
                .mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, String>() {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                        String monitorId = tuple._1;
                        Iterator<String> cameraIterator = tuple._2.iterator();
                        int count = 0;
                        StringBuilder cameraIds = new StringBuilder();
                        while (cameraIterator.hasNext()) {
                            cameraIds.append("," + cameraIterator.next());
                            count++;
                        }
                        String cameraInfos = Constants.FIELD_CAMERA_IDS + "=" + cameraIds.toString().substring(1) + "|" + Constants.FIELD_CAMERA_COUNT + "=" + count;
                        return new Tuple2<>(monitorId, cameraInfos);
                    }
                });


        /**
         * 将两个RDD进行比较，join  leftOuterJoin
         * 为什么使用左外连接？ 左：标准表里面的信息  右：实际信息
         * key:m_id, value(standInfo,factInfo)
         */
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joinResultRDD =
                standardMonitor2CameraInfos.leftOuterJoin(monitorId2CameraCountRDD);

        /**
         * carCount2MonitorId 最终返回的K,V格式的数据
         * K：实际监测数据中某个卡扣对应的总车流量
         * V：实际监测数据中这个卡扣 monitorId
         */


        JavaPairRDD<Integer, String> carCount2MonitorId = joinResultRDD.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Tuple2<String, Tuple2<String, Optional<String>>>>, Integer, String>() {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<Integer, String>> call(
                            //String, Tuple2<String, Optional<String>>
                            Iterator<Tuple2<String, Tuple2<String, Optional<String>>>> iterator) throws Exception {

                        List<Tuple2<Integer, String>> list = new ArrayList<>();
                        while (iterator.hasNext()) {
                            //储藏返回值 (0001,(cameraIds=02322,01213,03442|cameraCount=3,cameraIds=02322,01213,03442|cameraCount=3|carcount=100...
                            Tuple2<String, Tuple2<String, Optional<String>>> tuple = iterator.next();
                            String monitorId = tuple._1;
                            String standardCameraInfos = tuple._2._1;
                            Optional<String> factCameraInfosOptional = tuple._2._2;
                            String factCameraInfos ;

                            if (factCameraInfosOptional.isPresent()) {
                                //这里面是实际检测数据中有标准卡扣信息
                                factCameraInfos = factCameraInfosOptional.get();
                            } else {
                                //这里面是实际检测数据中没有标准卡扣信息
                                String standardCameraIds =
                                        StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);
                                String[] split = standardCameraIds.split(",");
                                int abnoramlCameraCount = split.length;

                                StringBuilder abnormalCameraInfos = new StringBuilder();
                                for (String cameraId : split) {
                                    abnormalCameraInfos.append("," + cameraId);
                                }
                                //abnormalMonitorCount=1|abnormalCameraCount=10|abnormalMonitorCameraInfos="0002":07553,07554,07556
                                accumulator.add(
                                        Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|"
                                                + Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnoramlCameraCount + "|"
                                                + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + monitorId + ":" + abnormalCameraInfos.substring(1));
                                //跳出了本次while
                                continue;
                            }

                            /**
                             * 从实际数据拼接的字符串中获取摄像头数
                             * cameraIds=02322,01213,03442|cameraCount=3|carcount=100...
                             */
                            int factCameraCount = Integer.parseInt(StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT));
                            /**
                             * 从标准数据拼接的字符串中获取摄像头数
                             */
                            int standardCameraCount = Integer.parseInt(StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_COUNT));
                            if (factCameraCount == standardCameraCount) {
                        /*
                         * 	1、正常卡口数量
						 * 	2、异常卡口数量
						 * 	3、正常通道（此通道的摄像头运行正常）数，通道就是摄像头
						 * 	4、异常卡口数量中哪些摄像头异常，需要保存摄像头的编号  
						*/
                                accumulator.add(Constants.FIELD_NORMAL_MONITOR_COUNT + "=1|" +
                                        Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + factCameraCount);
                            } else {
                                /**
                                 * 从实际数据拼接的字符串中获取摄像编号集合
                                 */
                                String factCameraIds = StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);

                                /**
                                 * 从标准数据拼接的字符串中获取摄像头编号集合
                                 */
                                String standardCameraIds = StringUtils.getFieldFromConcatString(standardCameraInfos, "\\|", Constants.FIELD_CAMERA_IDS);

                                List<String> factCameraIdList = Arrays.asList(factCameraIds.split(","));
                                List<String> standardCameraIdList = Arrays.asList(standardCameraIds.split(","));
                                StringBuilder abnormalCameraInfos = new StringBuilder();
//						System.out.println("factCameraIdList:"+factCameraIdList);
//						System.out.println("standardCameraIdList:"+standardCameraIdList);
                                int abnormalCmeraCount = 0;//不正常摄像头数
                                int normalCameraCount = 0;//正常摄像头数
                                for (String str : standardCameraIdList) {
                                    if (!factCameraIdList.contains(str)) {
                                        abnormalCmeraCount++;
                                        abnormalCameraInfos.append("," + str);
                                    }
                                }

                                normalCameraCount = standardCameraIdList.size() - abnormalCmeraCount;

                                //往累加器中更新状态
                                accumulator.add(
                                        Constants.FIELD_NORMAL_CAMERA_COUNT + "=" + normalCameraCount + "|"
                                                + Constants.FIELD_ABNORMAL_MONITOR_COUNT + "=1|"
                                                + Constants.FIELD_ABNORMAL_CAMERA_COUNT + "=" + abnormalCmeraCount + "|"
                                                + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS + "=" + monitorId + ":" + abnormalCameraInfos.toString().substring(1));
                            }
                            //从实际数据拼接到字符串中获取车流量
                            int carCount = Integer.parseInt(StringUtils.getFieldFromConcatString(factCameraInfos, "\\|", Constants.FIELD_CAR_COUNT));
                            list.add(new Tuple2<>(carCount, monitorId));
                        }
                        //最后返回的list是实际监测到的数据中，list[(卡扣对应车流量总数,对应的卡扣号),... ...]
                        return list.iterator();
                    }
                });
        return carCount2MonitorId;
    }

}


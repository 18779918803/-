package com.mage.spark.skynet;

import com.mage.spark.util.DateUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 获取指定卡口号下所有车辆的行车轨迹
 */
public class CarsTrack {
    public static void main(String[] args) {
        //第一步要获取你的环境
        SparkSession sparkSession = getSession("carTrack");
        JavaRDD<Row> carRDD = getRowData(sparkSession, "2018-06-27", "2018-06-27");
        //车辆的列表
        List<String> carsList = getCarListByRdd(carRDD, "0001").collect();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        final Broadcast<List<String>> listBroadcast = jsc.broadcast(carsList);
        //最后一部分，将你的广播变量和一个rdd去做filter，拿到行车轨迹最终
        getCarTracks(carRDD,listBroadcast);

    }

    /**
     * 将你的广播变量和你传入的rdd进行过滤操作，拿到所有的数据提取出其中的轨迹（即卡口号），内部实现row数据间的比较规则（时间）
     * @param carRDD
     * @param listBroadcast
     */
    private static void getCarTracks(JavaRDD<Row> carRDD, Broadcast<List<String>> listBroadcast) {
        carRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(3),row);
            }
        }).filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                List<String> carsList = listBroadcast.value();
                return carsList.contains(v1._2().getString(3));
            }
        }).groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Row>>>() {
            //(car,[row,row....])
            @Override
            public void call(Tuple2<String, Iterable<Row>> stringIterableTuple2) throws Exception {
                String car = stringIterableTuple2._1;
                Iterator<Row> iterator = stringIterableTuple2._2.iterator();
//                String track = "";
                StringBuilder track = new StringBuilder();
                List<Row> list = IteratorUtils.toList(iterator);
                Collections.sort(list, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String date1 = o1.getString(4);
                        String date2 = o2.getString(4);
                        boolean after = DateUtils.after(date1, date2);
                        if(after){
                            return 1;
                        }else{
                            if(date1.equals(date2)){
                               return 0;
                            }else{
                                return -1;
                            }
                        }
                    }
                });
                for(Row row:list){
                    //我所需要的是row里面的轨迹，即卡口号
//                    track = track + ">>" +row.getString(1);
                    track.append(">>"+row.getString(1));
                }
                System.out.println(car+":"+track.substring(2));
            }
        });
    }

    /**
     * 根据传入的指定卡口号对rdd进行过滤，返回值为要变成广播变量的rdd即carList
     * @param carRDD
     * @param monitorId
     * @return
     */
    private static JavaRDD<String> getCarListByRdd(JavaRDD<Row> carRDD, String monitorId) {
        //数据进来的格式是row，要变成(mid,row)
        JavaRDD<String> carsRdd = carRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(1), row);
            }
        }).filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                return v1._1().equals(monitorId);
            }
        }).map(new Function<Tuple2<String, Row>, String>() {
            @Override
            public String call(Tuple2<String, Row> v1) throws Exception {
                return v1._2().getString(3);
            }
        }).distinct();
        return carsRdd;
    }

    /**
     * 根据日期参数从sparkSession中获取数据对象并且转换成rdd对象
     * @param sparkSession
     * @param startDate
     * @param endDate
     * @return
     */
    private static JavaRDD<Row> getRowData(SparkSession sparkSession, String startDate, String endDate) {
        String sql =
                "SELECT * "
                        + "FROM monitor_flow_action "
                        + "WHERE date>='" + startDate + "' "
                        + "AND date<='" + endDate + "'";

        Dataset<Row> monitorDF = sparkSession.sql(sql);

        return monitorDF.javaRDD();
    }
    /**
     * 创建指定的任务对应的session对象
     * @param appName
     * @return
     */
    public static SparkSession getSession(String appName) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName(appName)
                .master("local")
                .getOrCreate();
        //要拿到数据（rdd）
        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        JavaRDD<String> lineRdd = jsc.textFile("./monitor_flow_action");
        JavaRDD<Row> rowRdd = lineRdd.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                return RowFactory.create(
                        line.split("\\t")[0],
                        line.split("\\t")[1],
                        line.split("\\t")[2],
                        line.split("\\t")[3],
                        line.split("\\t")[4],
                        line.split("\\t")[5],
                        line.split("\\t")[6],
                        line.split("\\t")[7]
                );
            }
        });
        StructType cameraFlowSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
                DataTypes.createStructField("camera_id", DataTypes.StringType, true),
                DataTypes.createStructField("car", DataTypes.StringType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("speed", DataTypes.StringType, true),
                DataTypes.createStructField("road_id", DataTypes.StringType, true),
                DataTypes.createStructField("area_id", DataTypes.StringType, true)
        ));
        Dataset<Row> df = sparkSession.createDataFrame(rowRdd, cameraFlowSchema);
        df.show();
        df.createOrReplaceTempView("monitor_flow_action");
        return sparkSession;
    }
}

package com.mage.spark.skynet;


import com.mage.spark.context.TrafficContext;
import com.mage.spark.util.DateUtils;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * 0001卡口下的车辆行车轨迹....
 */

public class CarTrack0001 {

    public static void main(String[] args) {

        SparkSession sparkSession = TrafficContext.initContext("CarTrack0001");

        JavaRDD<Row> cameraRDD = getData(sparkSession, "2019-03-26","2019-03-26");

        List<String> carsList = getCars(cameraRDD, "0001").collect();
//      将车辆列表广播出去
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());

        final Broadcast<List<String>> car_broadcast = sc.broadcast(carsList);

        getCarTrack(cameraRDD,car_broadcast);
    }

    /**
     * 获取日期范围内的数据...
     * @param sparkSession
     * @param startDate
     * @param endDate
     * @return
     */
    public static JavaRDD<Row> getData(SparkSession sparkSession,String startDate,String endDate){


        String sql =
                "SELECT * "
                        + "FROM monitor_flow_action "
                        + "WHERE date>='" + startDate + "' "
                        + "AND date<='" + endDate + "'";

        Dataset<Row> monitorDF = sparkSession.sql(sql);

        return monitorDF.javaRDD();
    }


    /**
     * @param
     * @return void
     * @throws
     * @Description: 获取某个卡扣下的所有车辆
     * @date 2018/7/30 0030
     * @date 15:40
     * @author lsw
     */
    private static JavaRDD<String> getCars(JavaRDD<Row> javaRDD,  String monitorid) {
        //数据格式：<monitorId,row>
        JavaPairRDD<String, Row> monitorid_row_pair = javaRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(1), row);
            }
        });
//    数据过滤 ,数据去重

        JavaRDD<String> cars = monitorid_row_pair.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                return v1._1().equals(monitorid);
            }
        }).map(new Function<Tuple2<String, Row>, String>() {
            @Override
            public String call(Tuple2<String, Row> v1) throws Exception {
                return v1._2().getString(3);
            }
        }).distinct();
        return cars;
    }



    private static void getCarTrack(JavaRDD<Row> javaRDD, final Broadcast<List<String>> car_broadcast) {
//      JavaSparkContext jsc = new JavaSparkContext(javaRDD.context());

        javaRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(3), row);
            }
        }).filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v1) throws Exception {
                List<String> car_list = car_broadcast.value();

                return car_list.contains(v1._2().getString(3));
            }
        }).groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Row>>>() {
            //          <car,[row,row]>
            @Override
            public void call(Tuple2<String, Iterable<Row>> tuple2Iterator) throws Exception {
//
                String car = tuple2Iterator._1;
                String track = "";
                Iterable<Row> rows1 = tuple2Iterator._2;
                Iterator<Row> iterator = rows1.iterator();
                List<Row> list = IteratorUtils.toList(iterator);

                Collections.sort(list, new Comparator<Row>() {
                    @Override
                    public int compare(Row o1, Row o2) {
                        String date1 = o1.getString(4);
                        String date2 = o2.getString(4);

                        boolean after = DateUtils.after(date1, date2);
                        if (after) {
                            return 1;
                        } else {

                            if (date1.equals(date2)) {
                                return 0;
                            } else {
                                return -1;
                            }

                        }

                    }
                });

                for (Row row : list) {
                    track = track + "->" + row.getString(1);
                }

                System.out.println(car + " : " + track.substring(2));
            }
        });

    }
}

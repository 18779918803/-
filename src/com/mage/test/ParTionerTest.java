package com.mage.test;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class ParTionerTest {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("partioner");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> parallelize = sc.parallelize(Arrays.asList(
                "a", "a", "b", "c",
                "a", "a", "b", "c"),2);

        parallelize.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).reduceByKey(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                if("a".equals(key)){
                    return 0;
                }else if("b".equals(key)){
                    return 1;
                }else{
                    return 2;
                }

            }

            @Override
            public int numPartitions() {
                return 3;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });


    }
}

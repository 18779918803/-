package com.mage.test;

import java.util.*;
import java.util.Map.Entry;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.mage.spark.util.DateUtils;
import com.mage.spark.util.StringUtils;



/**
 * 模拟数据  数据格式如下：
 * 
 *  日期	      卡口ID		     摄像头编号  	车牌号	       拍摄时间	              车速	             道路ID   	   区域ID
 * date	 monitor_id	         camera_id	    car	        action_time		         speed	            road_id		  area_id
 * 
 * monitor_flow_action
 * monitor_camera_info
 * 
 * @author Administrator
 */
public class MockData {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("mockdata")
                .master("local")
                .getOrCreate();
        mock(sparkSession);

    }

    public static void mock(SparkSession sparkSession) {
        List<Row> dataList = new ArrayList<Row>();
        Random random = new Random();

        String[] locations = new String[]{"浙","京","京","京","沪","沪","沪","沪","闵","京"};
//     	String[] areas = new String[]{"黄浦区","徐汇区","长宁区","松江区","浦东区","金山区","嘉定区","青浦区"};
        //date :如：2019-12-31
        String date = DateUtils.getTodayDate();
        /**
         * 模拟3000个车辆
         */
        for (int i = 0; i < 3000; i++) {
            //模拟车牌号：如：沪A00001
            String car = locations[random.nextInt(10)] + (char)(65+random.nextInt(26))+StringUtils.fulfuill(5,random.nextInt(99999)+"");
            //baseActionTime 模拟24小时 2017-08-08 01
            String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24)+"");
            /**
             * 这里的for循环模拟每辆车经过不同的卡扣不同的摄像头 数据。
             */
            int number = random.nextInt(300);

            for(int j = 0 ; j < number ; j++){
//                //模拟每个车辆每被30个摄像头拍摄后 时间上累计加1小时。这样做使数据更加真实。
                if(j % 30 == 0 && j != 0){
                    baseActionTime = date + " " + StringUtils.fulfuill((Integer.parseInt(baseActionTime.split(" ")[1])+1)+"");
                }
                String actionTime = baseActionTime + ":"
                        + StringUtils.fulfuill(random.nextInt(60)+"") + ":"
                        + StringUtils.fulfuill(random.nextInt(60)+"");//模拟经过此卡扣开始时间 ，如：2017-10-01 20:09:10
                String monitorId = StringUtils.fulfuill(4, random.nextInt(9)+"");//模拟9个卡扣monitorId，0补全4位
                String speed = random.nextInt(200)+"";//模拟速度
                String roadId = random.nextInt(50)+1+"";//模拟道路id 【1~50 个道路】
                String cameraId = StringUtils.fulfuill(5, random.nextInt(9999)+"");//模拟摄像头id cameraId
                String areaId = StringUtils.fulfuill(2,random.nextInt(8)+1+"");//模拟areaId 【一共8个区域】

                Row row = RowFactory.create(date,monitorId,cameraId,car,actionTime,speed,roadId,areaId);
                dataList.add(row);
            }
        }

        /**
         * 2018-4-20 1	22	沪A1234
         * 2018-4-20 1	23	京A1234
         * 1 【22,23】
         * 1 【22,23,24】
         */

        SparkContext sparkContext = sparkSession.sparkContext();

        JavaSparkContext sc = new JavaSparkContext(sparkContext);

        JavaRDD<Row> rowRdd = sc.parallelize(dataList);

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
        //模拟数据作为原数据，并且通过动态创建schema的方式，将原数据变成了二维表。
        Dataset<Row> df = sparkSession.createDataFrame(rowRdd, cameraFlowSchema);

        //默认打印出来df里面的20行数据
        System.out.println("----打印 车辆信息数据----");
        df.show();
        df.registerTempTable("monitor_flow_action");


        /**
         * monitorAndCameras    key：monitor_id
         * 						value:hashSet(camera_id)
         * 基于生成的数据，生成对应的卡扣号和摄像头对应基本表
         */
        Map<String,Set<String>> monitorAndCameras = new HashMap<>();

        int index = 0;
        for(Row row : dataList){
            //row.getString(1)  0001    monitor_id
            Set<String> sets = monitorAndCameras.get(row.getString(1));
            if(sets == null){
                sets = new HashSet<>();
                monitorAndCameras.put(row.getString(1), sets);
            }
            //这里每隔1000条数据随机插入一条数据，模拟出来标准表中卡扣对应摄像头的数据比模拟数据中多出来的摄像头。这个摄像头的数据不一定会在车辆数据中有。即可以看出卡扣号下有坏的摄像头。
            index++;
            if(index % 1000 == 0){
                sets.add(StringUtils.fulfuill(5, random.nextInt(99999)+""));
            }
            //row.getString(2) camera_id
            //2018-06-27	0008	01759	京H26555	2018-06-27 15:36:53	210	44	03
            sets.add(row.getString(2));
        }


        dataList.clear();

        Set<Entry<String,Set<String>>> entrySet = monitorAndCameras.entrySet();
        for (Entry<String, Set<String>> entry : entrySet) {
            String monitor_id = entry.getKey();
            Set<String> sets = entry.getValue();
            Row row ;
            for (String val : sets) {
                row = RowFactory.create(monitor_id,val);
                dataList.add(row);
            }
        }

        StructType monitorSchema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("monitor_id", DataTypes.StringType, true),
                DataTypes.createStructField("camera_id", DataTypes.StringType, true)
        ));


        rowRdd = sc.parallelize(dataList);

        Dataset<Row> monitorDF = sparkSession.createDataFrame(rowRdd, monitorSchema);

        monitorDF.createOrReplaceTempView("monitor_camera_info");

        System.out.println("----打印 卡扣号对应摄像头号 数据----");

        monitorDF.show();
    }
}















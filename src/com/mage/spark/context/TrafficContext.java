package com.mage.spark.context;

import com.alibaba.fastjson.JSONObject;
import com.mage.spark.conf.ConfigurationManager;
import com.mage.spark.constant.Constants;
import com.mage.spark.dao.ITaskDAO;
import com.mage.spark.dao.factory.DAOFactory;
import com.mage.spark.domain.Task;
import com.mage.spark.util.ParamUtils;
import com.mage.spark.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;


public class TrafficContext {


    /**
     * 初始化车流量项目的上下文环境
     * 1.本地运行，就模拟数据
     * 2.集群运行，则数据源是hive上的表
     * @return
     */
    public static SparkSession initContext(String appName){

        // 构建Spark运行时的环境参数
        SparkConf conf = new SparkConf()
                .setAppName(appName)
//			.set("spark.sql.shuffle.partitions", "300")
//			.set("spark.default.parallelism", "100")
//			.set("spark.storage.memoryFraction", "0.5")
//			.set("spark.shuffle.consolidateFiles", "true")
//			.set("spark.shuffle.file.buffer", "64")
//			.set("spark.shuffle.memoryFraction", "0.3")
//			.set("spark.reducer.maxSizeInFlight", "96")
//			.set("spark.shuffle.io.maxRetries", "60")
//			.set("spark.shuffle.io.retryWait", "60")
//			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(new Class[]{SpeedSortKey.class})
                ;


        /**
         * 设置spark运行时的master  根据配置文件来决定的
         */
        SparkUtils.setMaster(conf);

        /**
         * 查看配置文件是否是本地测试，如果是集群运行，就开启hive支持，否则不开启
         */
        SparkSession sparkSession = SparkUtils.getSparkSession(conf);

        /**
         * 基于本地测试生成模拟测试数据，如果在集群中运行的话，直接操作Hive中的表就可以
         * 本地模拟数据注册成一张临时表
         * monitor_flow_action	数据表：监控车流量所有数据
         * monitor_camera_info	标准表：卡扣对应摄像头标准表
         */
        if (ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)) {
            //本地
            SparkUtils.mockData(sparkSession);
        } else {
            //集群
            sparkSession.sql("use traffic");
        }

        return sparkSession;

    }

    /**
     *
     * @param taskId
     * @return 获取taskId,所对应的任务参数..
     */

    public static JSONObject getTaskParams(long taskId){

        /**
         * 获取ITaskDAO的对象，通过taskId查询出来的数据封装到Task（自定义）对象
         */
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findTaskById(taskId);

        if (task == null) {
            System.err.println(" task is null ....");
            return null;
        }

        /**
         * task.getTaskParams()是一个json格式的字符串   封装到taskParamsJsonObject
         * 将 task_parm字符串转换成json格式数据。
         */
        JSONObject taskParamsJsonObject = JSONObject.parseObject(task.getTaskParams());

        return taskParamsJsonObject;
    }
    /**
     *
     * @param args  args 命令行参数
     *
     * @param taskType 参数类型 ，对应my.properties中任务相关的key 。
     *
     * @return 任务对应的参数..
     */
    public static Long getTaskId(String[] args,String taskType){
        //获取任务ID
        Long taskId = ParamUtils.getTaskIdFromArgs(args, taskType);

        return taskId;
    }
}

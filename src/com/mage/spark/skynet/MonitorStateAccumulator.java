package com.mage.spark.skynet;

import com.mage.spark.constant.Constants;
import com.mage.spark.util.StringUtils;
import org.apache.spark.util.AccumulatorV2;

/**
 * 自定义累加器要实现AccumulatorV2抽象类
 *
 * 用于统计卡口和摄像头的状态信息
 *
 * 对于自定义累加器：
 *     主要的方法是add和merge，这两个方法涉及到计算逻辑
 *     至于 isZero,copy,reset这三个方法。
 *        1.spark框架会先调用copy方法，克隆一个新的累加器accumulator
 *        2.对于新的累加器，调用reset方法，将copy生成的accumulator累加器的值重置成"zero value"初始值
 *        3.调用isZero方法，判断新创的累加器是不是处于初始值的状态，此时该方法必须返回true，否则报错：
 *             assertion failed: copyAndReset must return a zero value copy
 *             (AccumulatorV2.scala:168)
 *
 *
 *
 * @author root
 *
 */
public class MonitorStateAccumulator extends AccumulatorV2<String,String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	//累加器的初始值 zero value..
	public String result = Constants.FIELD_NORMAL_MONITOR_COUNT+"=0|"
            + Constants.FIELD_NORMAL_CAMERA_COUNT+"=0|"
            + Constants.FIELD_ABNORMAL_MONITOR_COUNT+"=0|"
            + Constants.FIELD_ABNORMAL_CAMERA_COUNT+"=0|"
            + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS+"= ";

    /**
     *  判断累加器的值是否处于初始值"zero value",若是则返回true,否则false
     *  注意：这里的"zero value" 不同累加器有不同含义
     *        对于数字累加器，"zero value" 一般是 0
     *        对于list累加器，"zero value" 一般是 长度为0的 list.
     *
     * @return
     */
    @Override
    public boolean isZero() {
       return result.equals(Constants.FIELD_NORMAL_MONITOR_COUNT+"=0|"
               + Constants.FIELD_NORMAL_CAMERA_COUNT+"=0|"
               + Constants.FIELD_ABNORMAL_MONITOR_COUNT+"=0|"
               + Constants.FIELD_ABNORMAL_CAMERA_COUNT+"=0|"
               + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS+"= ");
    }

    /**
     * 框架通过copy方法，生成新的累加器
     * @return
     */
    @Override
    public AccumulatorV2<String, String> copy() {

        MonitorStateAccumulator accumulator = new MonitorStateAccumulator();

        accumulator.result = this.result;

        return accumulator;
    }

    /**
     * 重置累加器的值，变成初始值："zero value"
     *
     */
    @Override
    public void reset() {
        result = Constants.FIELD_NORMAL_MONITOR_COUNT+"=0|"
                + Constants.FIELD_NORMAL_CAMERA_COUNT+"=0|"
                + Constants.FIELD_ABNORMAL_MONITOR_COUNT+"=0|"
                + Constants.FIELD_ABNORMAL_CAMERA_COUNT+"=0|"
                + Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS+"= ";
    }

    /**
     * 输入一个值，调用本方法做累加
     * 注意： 在这里，我们做的是字符串的累加
     * @param v
     */
    @Override
    public void add(String v) {
        //自己实现两字符串累加的逻辑
        myAdd(v);
    }

    /**
     *
     * 和其他相同的累加器做合并
     *
     * 注意：此时应该把每个累加器看做是各自不同分区的计算结果值，然后做总累加.
     * @param other
     */
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        MonitorStateAccumulator otherAccumulator = (MonitorStateAccumulator)other ;

        String otherResult = otherAccumulator.result;

        myAdd(otherResult);
    }

    private void myAdd(String v){
        //如果字符串为空，则返回
        if(StringUtils.isEmpty(v)){
            return ;
        }
        //abnormalMonitorCount=0|abnormalCameraCount=0|abnormalMonitorCameraInfos=""
        //result : abnormalMonitorCount=40|abnormalCameraCount=100|abnormalMonitorCameraInfos="0002":07553,07554~0004:8979,7987
        //v : abnormalMonitorCount=1|abnormalCameraCount=20|abnormalMonitorCameraInfos="0003":07544,07588,07599
        String[] valArr = v.split("\\|");
        for (String string : valArr) {
            String[] fieldAndValArr = string.split("=");
            String field = fieldAndValArr[0];
            String value = fieldAndValArr[1];
            String oldVal = StringUtils.getFieldFromConcatString(result, "\\|", field);
            if(oldVal != null){
                //只有这个字段是string，所以单独拿出来
                if(Constants.FIELD_ABNORMAL_MONITOR_CAMERA_INFOS.equals(field)){
                    result = StringUtils.setFieldInConcatString(result, "\\|", field, oldVal + "~" + value);
                }else{
                    //其余都是int类型，直接加减就可以
                    int newVal = Integer.parseInt(oldVal)+Integer.parseInt(value);
                    result = StringUtils.setFieldInConcatString(result, "\\|", field, String.valueOf(newVal));
                }
            }
        }
    }

    /**
     * Defines the current value of this accumulator
     * 返回累加器当前值
     * @return
     */
    @Override
    public String value() {
        return this.result;
    }
}

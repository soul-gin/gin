package com.gin.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;


/**
 * Table API
 * 在flink中创建一张表有两种方法:
 * 1. 从一个文件中导入表结构(structure)(常用于批计算)(静态)
 * 2. 从DataStream(流计算)或者DataSet转换成Table(动态)
 *
 * 从DataStream中创建Table（动态表）
 *
 * @author gin
 * @date 2021/3/10
 */
public class CreateTableEnvironment {

    public static void main(String[] args) {

        //创建流式计算的上下文环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建Table API的上下文环境
        //方式1
        StreamTableEnvironment tableStream1 = getTableStream1(streamEnv);

        //方式2(API丰富, 但稳定性待考量, 2020年初blink贡献至flink)
        StreamTableEnvironment tableStream2 = getTableStream2(streamEnv);

        //备用隐式转换
        //import org.apache.flink.table.api.java.*;
        //import org.apache.flink.streaming.api.*;

    }

    private static StreamTableEnvironment getTableStream2(StreamExecutionEnvironment streamEnv) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                //是否使用阿里巴巴的BlinkAPI
                .useBlinkPlanner()
                //是否用于流计算(不配置默认批计算)
                .inStreamingMode()
                .build();

        return StreamTableEnvironment.create(streamEnv, settings);
    }

    private static StreamTableEnvironment getTableStream1(StreamExecutionEnvironment streamEnv) {
        return StreamTableEnvironment.create(streamEnv);
    }

}

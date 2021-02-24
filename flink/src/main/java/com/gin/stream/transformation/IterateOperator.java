package com.gin.stream.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author gin
 * @date 2021/2/22
 * <p>
 * 基本概念: 在流中创建"反馈(feedback)"循环, 通过将一个算子的输出重定向到某个先前的算子
 * 这对于定义不断更新模型的算法特别有用(回测到模型计算结果符合要求)
 * <p>
 * 迭代的数据流向：DataStream → IterativeStream → DataStream
 */
public class IterateOperator {
    public static void main(String[] args) {
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 连接socket获取输入的数据, 输入数据应为数字类型
        // nc -lk 8888
        // netstat -natp | grep 8888
        DataStreamSource<String> stream = env.socketTextStream("node01", 8888, "\n");

        //将字符串转换为 Long 类型流
        SingleOutputStreamOperator<Long> longStream = stream.map(new MapFunction<String, Long>() {
            @Override
            public Long map(String resourceStr) throws Exception {
                return Long.parseLong(resourceStr);
            }
        });

        //获取迭代器
        IterativeStream<Long> iteration = longStream.iterate();
        SingleOutputStreamOperator<Long> iterationBody = iteration
                .map(new MapFunction<Long, Long>() {
                    @Override
                    public Long map(Long resource) throws Exception {
                        //定义迭代逻辑
                        //普调降3
                        if (resource > 0) {
                            resource -= 3;
                        }
                        return resource;
                    }
                });
        SingleOutputStreamOperator<Long> feedback = iterationBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long resource) throws Exception {
                //当 > 0 继续返回到stream流中,当 <= 0 继续往下游发送
                return resource > 0;
            }
        });
        //这里设置feedback这个数据流是被反馈的通道，只要是 value>0 的数据都会被重新迭代计算
        iteration.closeWith(feedback);

        //其余元素将向下游转发,离开迭代
        DataStream<Long> output = iterationBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                //当 <= 0 继续往下游发送
                return value <= 0;
            }
        });
        output.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

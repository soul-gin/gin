package com.gin.demo;

import com.gin.common.CommonConstants;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author gin
 * @date 2021/2/22
 *
 * 定时器 应用场景：数据延迟   2s
 *
 * 银行对账系统
 *
 * app下单, 扣款成功
 * 真正成功:
 * app成功 且 银行对账成功(银行扣款完成)
 */
public class OverSpeedCarSendMsg {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //车速超过 100 发送延时消息
        //DataStreamSource<Tuple2<String, Integer>> carSpeedStream = env.fromElements(new Tuple2<String, Integer>("car1", 99), new Tuple2<String, Integer>("car2", 120),
                //new Tuple2<String, Integer>("car3", 90), new Tuple2<String, Integer>("car4", 101));

        // 连接socket获取输入的数据
        // nc -lk 8888
        // netstat -natp | grep 8888
        DataStreamSource<String> socketStream = env.socketTextStream("node01", 8888, "\n");

        SingleOutputStreamOperator<Tuple2<String, Integer>> carSpeedStream = socketStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String res) throws Exception {
                String[] split = res.split(CommonConstants.STR_BLANK);
                String carName = split[0];
                Integer carSpeed = Integer.parseInt(split[1]);
                return new Tuple2<>(carName, carSpeed);
            }
        });

        carSpeedStream.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, String>() {

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        long currentTime = ctx.timerService().currentProcessingTime();

                        System.out.println(value.f1);
                        //触发定时器逻辑: 车速超过 100
                        int overSpeed = 100;
                        if (value.f1 > overSpeed) {
                            //两秒后发送消息
                            long delayTime = currentTime + 2 * 1000;
                            //注册延时任务时间
                            ctx.timerService().registerProcessingTimeTimer(delayTime);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        //预警消息
                        String warnMsg = "warn...  time:" + timestamp + "  carID:" + ctx.getCurrentKey();
                        out.collect(warnMsg);
                    }
                })
                .print();

        try {
            // 测试数据
            // bmw 200
            // cc 100
            // 结果
            // 8> warn...  time:1613991005976  carID:(bmw)
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

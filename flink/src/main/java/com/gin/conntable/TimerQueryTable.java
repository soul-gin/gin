package com.gin.conntable;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 定时刷新配置文件加载
 * 对于维度表更新频率比较高并且对于查询维度表的实时性要求比较高
 * 实现方案：
 * 使用定时器，定时加载外部配置文件或者数据库
 *
 * @author gin
 * @date 2021/3/8
 */
public class TimerQueryTable {

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.registerCachedFile("./data/id2city", "id2city");
            env.setParallelism(1);
            SingleOutputStreamOperator<Integer> flatMap = env.socketTextStream("node01", 8888, "\n")
                    .flatMap(new FlatMapFunction<String, Integer>() {
                        @Override
                        public void flatMap(String value, Collector<Integer> out) throws Exception {
                            // netstat -natp |grep 8888
                            // nc -lk 8888
                            // 输入
                            // 001
                            // 002
                            // 009
                            String[] splits = value.split("\\s");
                            for (String id : splits) {
                                out.collect(Integer.parseInt(id));
                            }
                        }
                    });
            flatMap.map(new RichMapFunction<Integer, String>() {

                Map<Integer, String> id2CityMap = new HashMap<>(16);

                @Override
                public void open(Configuration parameters) throws Exception {
                    System.out.println("init data");
                    timeQuery();
                    ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                            new BasicThreadFactory.Builder().namingPattern("config-schedule-pool-%d").daemon(true).build());
                    executorService.scheduleWithFixedDelay(new Runnable() {
                        @Override
                        public void run() {
                            timeQuery();
                        }
                    }, 5, 5, TimeUnit.SECONDS);
                }

                private void timeQuery() {
                    try {
                        File id2city = new File("./data/id2city");
                        String str = FileUtils.readFileUtf8(id2city);
                        String[] fileSplit = str.split("\r\n");
                        for (String mapStr : fileSplit) {
                            String[] splits = mapStr.split(" ");
                            Integer id = Integer.parseInt(splits[0]);
                            String city = splits[1];
                            id2CityMap.put(id, city);
                        }
                    } catch (Exception e) {
                        System.err.println("timeQuery err:" + e);
                    }
                }

                @Override
                public String map(Integer value) throws Exception {
                    if (null == id2CityMap.get(value)) {
                        return "not found";
                    }
                    return id2CityMap.get(value);
                }

            }).print();

            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

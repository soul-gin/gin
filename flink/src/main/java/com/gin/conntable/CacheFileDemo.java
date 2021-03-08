package com.gin.conntable;

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

/**
 * @author gin
 * @date 2021/3/8
 */
public class CacheFileDemo {

    public static void main(String[] args) {

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.registerCachedFile("./data/id2city", "id2city");
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
                    File id2city = getRuntimeContext().getDistributedCache().getFile("id2city");
                    String str = FileUtils.readFileUtf8(id2city);
                    String[] fileSplit = str.split("\r\n");
                    for (String mapStr : fileSplit) {
                        String[] splits = mapStr.split(" ");
                        Integer id = Integer.parseInt(splits[0]);
                        String city = splits[1];
                        id2CityMap.put(id, city);
                    }
                }

                @Override
                public String map(Integer value) throws Exception {
                    if (null == id2CityMap.get(value)){
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

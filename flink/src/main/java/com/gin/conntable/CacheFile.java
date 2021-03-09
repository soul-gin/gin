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
 * 不变配置文件加载
 * 维度表信息基本不发生改变，或者发生改变的频率很低
 * 实现方案：采用Flink提供的CachedFile
 * Flink提供了一个分布式缓存（CachedFile），类似于hadoop，可以使用户在并行函数中很方便的
 * 读取本地文件，并把它放在TaskManager节点中，防止task重复拉取。
 * 此缓存的工作机制如下：
 * 程序注册一个文件或者目录(本地或者远程文件系统，例如hdfs或者s3)，
 * 通过ExecutionEnvironment注册缓存文件并为它起一个名称。
 * 当程序执行，Flink自动将文件或者目录复制到所有TaskManager节点的本地文件系统，仅会执行一次。
 * 用户可以通过这个指定的名称查找文件或者目录，然后从TaskManager节点的本地文件系统访问它
 *
 * @author gin
 * @date 2021/3/8
 */
public class CacheFile {

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

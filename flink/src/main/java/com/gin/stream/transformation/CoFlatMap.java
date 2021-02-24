package com.gin.stream.transformation;

import com.gin.common.CommonConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gin
 * @date 2021/2/22
 * <p>
 * 现有一个配置文件存储车牌号与车主的真实姓名
 * 通过数据流中的车牌号实时匹配出对应的车主姓名
 * （注意：配置文件可能实时改变）
 * 配置文件可能实时改变  读取配置文件的适合  readFile  readTextFile（读一次）
 * stream1.connect(stream2)
 */
public class CoFlatMap {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度3
        env.setParallelism(1);

        String filePath = "data/carId2Name";

        //每秒读取一次最新配置, 车牌号 与 名字映射关系流
        DataStreamSource<String> cardIdMapNameStream = env.readFile(new TextInputFormat(new Path(filePath)),
                filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000);
        // nc -lk 8888
        // 然后输入车牌号, 根据 data/carId2Name 文件映射
        DataStreamSource<String> cardIdStream = env.socketTextStream("node01", 8888);

        cardIdStream.connect(cardIdMapNameStream)
                .map(new CoMapFunction<String, String, String>() {
                    //仅测试用: 每一个thread都会保存一个hashMap集合,不建议
                    Map<String, String> hashMap = new HashMap<>(16);

                    //数据流, 只有在配置流加载完成才能从hashMap中获取到数据(车主名称)
                    @Override
                    public String map1(String value) {
                        String name = hashMap.get(value);
                        return StringUtils.isEmpty(name) ? "not found" : name;
                    }

                    //配置流
                    @Override
                    public String map2(String value) {
                        String[] split = value.split(CommonConstants.STR_BLANK);
                        hashMap.put(split[0], split[1]);
                        return value + "加载完毕...";
                    }
                })
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

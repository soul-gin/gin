package com.gin.stream.transformation;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;

/**
 * @author gin
 * @date 2021/2/22
 * split 拆分流
 */
public class SplitOperator {

    private static final String EVEN_NUMBERS = "evenNumbers";
    private static final String ODD_NUMBER = "OddNumber";

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //注意: SplitStream已被废弃,建议使用 SideOutput (SideOutputOperator)
            DataStreamSource<Long> longDataStream = env.generateSequence(1, 10);
            SplitStream<Long> splitStream = longDataStream.split(new OutputSelector<Long>() {
                @Override
                public Iterable<String> select(Long value) {
                    LinkedList<String> longs = new LinkedList<>();
                    //偶数分到一个流(evenNumbers)  奇数分到另外一个流(OddNumber)
                    int evenOperator = 2;
                    if (value % evenOperator == 0) {
                        longs.add(EVEN_NUMBERS);
                    } else {
                        longs.add(ODD_NUMBER);
                    }

                    return longs;
                }
            });

            //select 算子可以通过标签 获取指定流
            //打印偶数流
            splitStream.select(EVEN_NUMBERS).print().setParallelism(1);

            //执行任务
            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

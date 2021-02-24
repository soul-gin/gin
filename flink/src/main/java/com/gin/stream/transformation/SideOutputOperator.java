package com.gin.stream.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author gin
 * @date 2021/2/22
 *
 * SideOutputOperator, 将一个流拆分为"主输出流", "侧输出流"
 * 实际上在同一个流中, 通过tag区分
 * "主输出流"可直接输出, "侧输出流"需要获取输出
 */
public class SideOutputOperator {


    private static final String ODD_NUMBER = "OddNumber";

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //注意: SplitStream已被废弃,建议使用 SideOutput (SideOutputOperator)
            DataStreamSource<Long> longDataStream = env.generateSequence(1, 10);
            //定义侧输出流的tag标签
            //注意: 这里创建 OutputTag 时需要加 {} , 避免 Could not determine TypeInformation for the OutputTag type
            //因为flink要识别侧输出的数据类型, {} 是实现成了匿名内部类，泛型里就带类型了
            OutputTag<Long> outputTag = new OutputTag<Long>(ODD_NUMBER){};


            SingleOutputStreamOperator<Long> mainStream = longDataStream.process(new ProcessFunction<Long, Long>() {
                @Override
                public void processElement(Long value, Context ctx, Collector<Long> out) {
                    int evenOperator = 2;
                    if (value % evenOperator == 0) {
                        out.collect(value);
                    } else {
                        ctx.output(outputTag, value);
                    }
                }
            });



            //侧输出流需要通过tag获取
            DataStream<Long> sideStream = mainStream.getSideOutput(outputTag);
            sideStream.print("sideStream");

            //默认打印主输出流(偶数流)
            mainStream.print("mainStream").setParallelism(1);

            //执行任务
            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

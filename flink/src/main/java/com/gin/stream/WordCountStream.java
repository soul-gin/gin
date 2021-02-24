package com.gin.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author gin
 * @date 2021/02/20
 * <p>
 * 运行前先在node01上启动一个客户端通信端口
 * <p>
 * nc -lk 8888
 * <p>
 * nc命令安装：
 * yum install nc -y
 * yum install nmap -y
 */
public class WordCountStream {
    public static void main(String[] args) throws Exception {

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> wordCount = getWordCountStream(env);

        //把数据打印到控制台, 使用一个并行度
        wordCount.print().setParallelism(1);
        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        env.execute("streaming word count");

    }

    public static DataStream<Tuple2<String, Integer>> getWordCountStream(StreamExecutionEnvironment env) {
        //连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream("node01", 8888, "\n");

        //计算数据
        return text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        //按空格 TAB 换行切分(如: splits = value.split(" ")
                        String[] splits = value.split("\\s");
                        for (String word : splits) {
                            out.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })//打平操作，把每行的单词转为<word,count>类型的数据
                //针对相同的word数据进行分组
                .keyBy(0)
                //指定计算数据的窗口大小和滑动窗口大小
                //.timeWindow(Time.seconds(2), Time.seconds(1))
                .sum(1);
    }

}

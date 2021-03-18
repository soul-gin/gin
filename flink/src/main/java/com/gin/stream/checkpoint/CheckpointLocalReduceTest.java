package com.gin.stream.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

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
 *
 * reduce 中的 sum 算子已经集成了state(基于 keyBy 的 keyState)
 *
 * 测试数据:
 * [root@node01 ~]# nc -lk 8888
 * 1
 * 2 2
 * 3 3 3
 * 4 4 4 4 error
 * error 4 4 4 4
 * 4 4 4 4
 * error
 * 4
 * 4
 *
 * 输出结果:
 * (1,1)
 * (2,1)
 * (2,2)
 * (3,1)
 * (3,2)
 * (3,3)
 * error test, rollback
 * (4,1)
 * (4,2)
 * (4,3)
 * (4,4)
 * error test, rollback
 * (4,1)
 * (4,2)
 * (4,3)
 * (4,4)
 * error test, rollback
 * (4,1)
 * (4,2)
 *
 * 出现 RuntimeException , 任务重启, 状态回滚(4 重新计数)
 */
public class CheckpointLocalReduceTest {
    public static void main(String[] args) throws Exception {
        // checkpoint env
        StreamExecutionEnvironment env = CheckPointEnvUtil.getStreamExecEnv();

        env.setParallelism(1);
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
                            if ("error".equals(word)) {
                                System.out.println("error test, rollback");
                                throw new RuntimeException("error test");
                            }
                            out.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })//打平操作，把每行的单词转为<word,count>类型的数据
                //针对相同的word数据进行分组
                .keyBy(0)
                // reduce 中的 sum 算子已经集成了state(基于 keyBy 的 keyState)
                .sum(1)
                //设置 checkpoint 的ID, 代码升级时, 快速定位需要恢复的id
                //如果不指定, flink会随机生成, 不方便指定checkpoint id进行恢复
                //如果当前代码之后会变动, 那么就需要在当前代码位置设置一个id
                .uid("reduce checkpoint id");
    }

}

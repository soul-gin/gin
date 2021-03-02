package com.gin.stream.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
public class CheckpointTest {
    public static void main(String[] args) throws Exception {

        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置状态后端  所谓状态后端就是数据保存位置
        //设置是否异步持久化数据, 文件系统状态后端(快照 和 keyState): FsStateBackend; (MemoryStateBackend, FsStateBackend, RocksDBStateBackend)
        env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink/checkpoint",true));

        //每隔 1000ms 往数据源中插入一个barrier(批次标志位)
        //注意: 开启了 enableCheckpointing, 则 setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) 会默认开启
        env.enableCheckpointing(1000);
        DataStream<Tuple2<String, Integer>> wordCount = getWordCountStream(env);

        //把数据打印到控制台, 使用一个并行度
        wordCount.print().setParallelism(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        //checkpoint超时时间  10分钟
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        //设置checkpoint模式, 默认就是 CheckpointingMode.EXACTLY_ONCE (精确一次消费)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        /**
         * 设置checkpoint任务之间的间隔时间  checkpoint job1  checkpoint job2
         * 防止触发太密集的flink checkpoint，导致消耗过多的flink集群资源
         * 导致影响整体性能
         * 600ms
         * 注意: 设置了这个参数, 表示checkpoint就应该是串行执行的;
         * 即: setMaxConcurrentCheckpoints(1), 否则当前配置会失效
         */
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(600);
        //设置checkpoint最大并行的个数   3checkpoint job
        // 1 表示串行执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //flink 任务取消之后，checkpoint数据是否删除
        // RETAIN_ON_CANCELLATION 当任务取消，checkpoints数据会保留
        // DELETE_ON_CANCELLATION 当任务取消，checkpoints数据会删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        //注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
        //测试:
        // 启动任务
        // flink run -c com.gin.stream.checkpoint.CheckpointTest ./flink-1.0-SNAPSHOT.jar
        // 取消任务, ctrl+c 或者页面取消
        // 使用 checkpoint 重启任务
        // 注意: 对应的是包含 _metadata 的目录
        // flink run -c com.gin.stream.checkpoint.CheckpointTest -s hdfs://node01:8020/flink/checkpoint/bb7169eb928edcc86f06a630a3ac6689/chk-87 ./flink-1.0-SNAPSHOT.jar
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
                .sum(1)
                //设置 checkpoint 的ID, 代码升级时, 快速定位需要恢复的id
                //如果不指定, flink会随机生成, 不方便指定checkpoint id进行恢复
                //如果当前代码之后会变动, 那么就需要在当前代码位置设置一个id
                .uid("reduce checkpoint id");
    }

}

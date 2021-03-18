package com.gin.stream.checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author gin
 * @date 2021/3/18
 */
public class CheckPointEnvUtil {

    public static StreamExecutionEnvironment getStreamExecEnv() {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置状态后端: 数据保存位置
        //设置缓存大小(默认5M), 是否异步持久化数据, MemoryStateBackend, FsStateBackend, RocksDBStateBackend
        env.setStateBackend(new MemoryStateBackend(5 * 1024 * 1024, false));
        //每隔 5000ms 往数据源中插入一个barrier(批次标志位)
        //开启了 enableCheckpointing, 则 setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) 会默认开启
        env.enableCheckpointing(5000);
        //设置checkpoint模式, 默认就是 CheckpointingMode.EXACTLY_ONCE (精确一次消费)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        /*
         * 设置checkpoint任务之间的间隔时间  checkpoint job1  checkpoint job2
         * 防止触发太密集的flink checkpoint，导致消耗过多的flink集群资源
         * 导致影响整体性能
         * 500ms
         * 注意: 设置了这个参数, 表示checkpoint就应该是串行执行的;
         * 即: setMaxConcurrentCheckpoints(1), 否则当前配置会失效
         */
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //设置checkpoint最大并行的个数   3checkpoint job
        // 1 表示串行执行, > 1表示并行执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //容忍checkpoint失败的次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        //checkpoint超时时间: 1分钟(检查点必须在一分钟内完成，或者被丢弃)
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        //flink 任务取消之后，checkpoint数据是否删除
        // RETAIN_ON_CANCELLATION 当任务取消，checkpoints数据会保留
        // DELETE_ON_CANCELLATION 当任务取消，checkpoints数据会删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        // 重启策略的配置
        // 重启3次，每次失败后等待10000毫秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        // 在5分钟内，只能重启5次，每次失败后最少需要等待10秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));
        return env;
    }

}

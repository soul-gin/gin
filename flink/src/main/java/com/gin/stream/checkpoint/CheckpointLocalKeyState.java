package com.gin.stream.checkpoint;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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
 * <p>
 * 测试数据:
[root@node01 ~]# nc -lk 8888
1
2 2
3 3 3
4 4 4 4 error
error 4 4 4 4
4 4 4 4
error
4
4
 * <p>
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
 * <p>
 * 出现 RuntimeException , 任务重启, 状态回滚(4 重新计数)
 */
public class CheckpointLocalKeyState {
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
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    //按空格 TAB 换行切分(如: splits = value.split(" ")
                    String[] splits = value.split("\\s");
                    for (String word : splits) {
                        if ("error".equals(word)) {
                            System.out.println("error test, rollback");
                            throw new RuntimeException("error test");
                        }
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }))
                //设置 checkpoint 的ID, 代码升级时, 快速定位需要恢复的id
                //如果不指定, flink会随机生成, 不方便指定checkpoint id进行恢复
                //如果当前代码之后会变动, 那么就需要在当前代码位置设置一个id
                .uid("map-checkpoint-id")
                .keyBy(0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    /**
                     * 历史状态
                     */
                    private ValueState<Integer> latestState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //2天过期
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(2))
                                //过期不可访问
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                //在状态值被创建和被更新时重设TTL
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();

                        // 指定 state 类型
                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("count-state",
                                TypeInformation.of(new TypeHint<Integer>() {
                                }));
                        // 注册 state
                        stateDescriptor.enableTimeToLive(stateTtlConfig);
                        latestState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        Integer res = latestState.value();
                        if (null == res) {
                            res = new Integer(0);
                        }
                        res++;
                        latestState.update(res);
                        System.out.printf("word=%s wordTotalCount=%s \n", value.f0, res);
                        value.setField(res, 1);
                        return value;
                    }
                });
    }

}

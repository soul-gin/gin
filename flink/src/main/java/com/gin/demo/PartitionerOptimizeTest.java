package com.gin.demo;

import com.gin.common.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author gin
 * @date 2021/2/22
 * <p>
 * 发生数据倾斜: shuffle rebalance
 * 提高并行度: shuffle rebalance
 * 降低并行度(节省网络IO): rescale
 * 广播冗余数据(场景:映射表,每个下游都需要一份一样的数据): broadcast
 */
public class PartitionerOptimizeTest {

    private static final String STREAM_PATH_ONE = "./data/stream1";
    private static final String STREAM_PATH_TWO = "./data/stream2";

    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //设置全局默认并行度
            env.setParallelism(4);
            System.out.println("全局默认分区数=" + env.getParallelism());
            //数据源 设置一个并行度
            DataStreamSource<Long> stream = env.generateSequence(1, 10).setParallelism(2);
            /*
            在同一个节点执行子任务, 可以避免网络传输数据, 随机分发难以保证
            但是大量数据落在同一个节点会出现数据倾斜
             */
            //shufflePartitioner(stream);
            //rebalancePartitioner(stream);
            //rescalePartitioner(stream);
            //rescalePartitioner2(stream);
            //broadcastPartitioner(stream);
            //globalPartitioner(stream);
            //forwardPartitioner(stream);
            //keyByPartitioner(stream);
            partitionCustomPartitioner(stream);

            env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void shufflePartitioner(DataStreamSource<Long> stream) {
        // 场景: 增大分区、提高并行度，解决数据倾斜
        // 特点: 分区元素随机均匀分发到下游分区，网络开销比较大
        // ShufflePartitioner 类中 selectChannel 方法:
        // 返回的是随机数 random.nextInt(numberOfChannels)
        stream.shuffle().print();
    }

    private static void rebalancePartitioner(DataStreamSource<Long> stream) {
        // 场景: 增大分区、提高并行度，解决数据倾斜
        // 特点: 轮询分区元素，均匀的将元素分发到下游分区，下游每个分区的数据比较均匀，在
        // 发生数据倾斜时非常有用，网络开销比较大
        // nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels
        // 取模方式实现
        stream.rebalance().print();
    }

    private static void rescalePartitioner(DataStreamSource<Long> stream) {
        // 场景: 减少分区 防止发生大量的网络传输 不会发生全量的重分区
        // 特点: 通过轮询分区元素，将一个元素集合从上游分区发送给下游分区，发送单位是集合，而不是一个个元素
        // 注意：rescale尽可能走本地数据传输，尽可能的不通过网络传输数据，比如taskmanager的槽数。
        // 简单来说，上游的数据只会发送给本TaskManager中的下游(flink槽数设置合理的情况下(slot))

        stream.rescale().print();
    }

    private static void rescalePartitioner2(DataStreamSource<Long> stream) {
        // 场景: 减少分区 防止发生大量的网络传输 不会发生全量的重分区
        // 特点: 通过轮询分区元素，将一个元素集合从上游分区发送给下游分区，发送单位是集合，而不是一个个元素
        // 注意：rescale尽可能走本地数据传输，尽可能的不通过网络传输数据，比如taskmanager的槽数。
        // 简单来说，上游的数据只会发送给本TaskManager中的下游(flink槽数设置合理的情况下(slot))

        //输入是两个分区
        //输出也是两个分区
        //故: 最终文件中是均分
        FileUtils.clearDir(STREAM_PATH_ONE, STREAM_PATH_TWO);
        stream.writeAsText(STREAM_PATH_ONE).setParallelism(2);
        stream.rescale().writeAsText(STREAM_PATH_TWO).setParallelism(4);
    }

    private static void broadcastPartitioner(DataStreamSource<Long> stream) {
        // 场景: 需要使用映射表、并且映射表会经常发生变动的场景
        // 特点: 上游中每一个元素内容广播到下游每一个分区中

        //输入是两个分区
        //输出是四个分区
        //故: 最终每个小文件中都包含了数据源的全量数据(场景: 映射表)
        FileUtils.clearDir(STREAM_PATH_ONE, STREAM_PATH_TWO);
        stream.writeAsText(STREAM_PATH_ONE).setParallelism(2);
        stream.broadcast().writeAsText(STREAM_PATH_TWO).setParallelism(4);
    }

    private static void globalPartitioner(DataStreamSource<Long> stream) {
        // 场景: 并行度降为1
        // 特点: 上游分区的数据只分发给下游的第一个分区

        //输入是两个分区
        //输出是一个分区
        FileUtils.clearDir(STREAM_PATH_ONE, STREAM_PATH_TWO);
        stream.writeAsText(STREAM_PATH_ONE).setParallelism(2);
        //即便设置了四个分区, 最终所有数据也是汇总至同一个分区
        stream.global().writeAsText(STREAM_PATH_TWO).setParallelism(4);
    }

    private static void forwardPartitioner(DataStreamSource<Long> stream) {
        // 场景: 一对一的数据分发，map、flatMap、filter 等都是这种分区策略
        // 特点: 上游分区数据分发到下游对应分区中

        //输入是两个分区
        //输出必须和输入同分区数: 两个分区
        FileUtils.clearDir(STREAM_PATH_ONE, STREAM_PATH_TWO);
        stream.writeAsText(STREAM_PATH_ONE).setParallelism(2);
        //必须保证上下游分区数（并行度）一致，不然会有如下异常:
        //Forward partitioning does not allow change of parallelism
        //stream.forward().writeAsText(STREAM_PATH_TWO).setParallelism(4)
        stream.forward().writeAsText(STREAM_PATH_TWO).setParallelism(2);
    }


    private static void keyByPartitioner(DataStreamSource<Long> stream) {
        // 场景: 与业务场景(key)匹配
        // 特点: 根据上游分区元素的Hash值与下游分区数取模计算出，将当前元素分发到下游哪一个分区

        //输入是两个分区
        //输出是四个分区
        FileUtils.clearDir(STREAM_PATH_ONE, STREAM_PATH_TWO);
        stream.writeAsText(STREAM_PATH_ONE).setParallelism(2);
        //MathUtils.murmurHash(keyHash)（每个元素的Hash值） % maxParallelism（下游分区数）
        stream.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long val) throws Exception {
                return new Tuple1(val);
            }
        }).keyBy(0).writeAsText(STREAM_PATH_TWO).setParallelism(4);
    }

    private static void partitionCustomPartitioner(DataStreamSource<Long> stream) {
        // 场景: 与业务场景(key)匹配
        // 特点: 根据上游分区元素的Hash值与下游分区数取模计算出，将当前元素分发到下游哪一个分区

        //输入是两个分区
        //输出是四个分区
        FileUtils.clearDir(STREAM_PATH_ONE, STREAM_PATH_TWO);
        stream.writeAsText(STREAM_PATH_ONE).setParallelism(2);
        //MathUtils.murmurHash(keyHash)（每个元素的Hash值） % maxParallelism（下游分区数）
        stream.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long val) throws Exception {
                return new Tuple1(val);
            }
        }).partitionCustom(new MyCustomPartitioner(), 0).writeAsText(STREAM_PATH_TWO).setParallelism(4);

    }



}

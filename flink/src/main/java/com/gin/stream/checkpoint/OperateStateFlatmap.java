package com.gin.stream.checkpoint;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author gin
 * @date 2021/3/18
 */
public class OperateStateFlatmap extends RichFlatMapFunction<String, Tuple2<String, Integer>>
        implements CheckpointedFunction {

    private transient volatile boolean restored;

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        Integer res = 0;
        if (null == res) {
            res = new Integer(0);
        }
        //按空格 TAB 换行切分(如: splits = value.split(" ")
        String[] splits = value.split("\\s");
        for (String word : splits) {
            if ("error".equals(word)) {
                System.out.println("error test, rollback");
                throw new RuntimeException("error test");
            }
            res++;
            System.out.printf("word=%s wordTotalCount=%s \n", word, res);
            out.collect(new Tuple2<>(word, 1));
        }

    }

    private transient ListState<Tuple2<String, Integer>> unionListState;
    private Map<String, Integer> wordCountMap;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        unionListState.clear();
        for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
            unionListState.add(Tuple2.of(entry.getKey(), entry.getValue()));
        }
    }
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        unionListState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>("operate-state",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                })));
        this.restored = context.isRestored();

        if (restored){
            System.out.println("restore...");
            if (wordCountMap == null) {
                wordCountMap = new ConcurrentHashMap<>();
            }
            for (Tuple2<String, Integer> oldVal : unionListState.get()) {
                if (!wordCountMap.containsKey(oldVal.f0) || wordCountMap.get(oldVal.f0) < oldVal.f1) {
                    wordCountMap.put(oldVal.f0, oldVal.f1);
                }
            }
        } else {
            System.out.println("not restore state...");
        }
    }


}

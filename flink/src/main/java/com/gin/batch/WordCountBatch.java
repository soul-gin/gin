package com.gin.batch;

import com.gin.common.CommonConstants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
* @author gin
* @date 2021/2/20
*/
public class WordCountBatch {



    public static void main(String[] args) {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            // 查看hdfs中hdfs-site.xml的 dfs.namenode.rpc-address 配置的端口
            /*
            <!-- nn1的RPC通信地址,8020取代了之前默认配置的 9000 端口,区分HA和standalone
                 netstat -ntp | grep 8020 -->
            <property>
              <name>dfs.namenode.rpc-address.mycluster.nn1</name>
              <value>node01:8020</value>
            </property>
             */
            DataSource<String> strDataSource = env.readTextFile("hdfs://node01:8020/data/wc/input");

            // "lambda+声明返回类型" 或 "匿名类+完整泛型"
            // 避免如下问题:
            // In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved.
            // Otherwise the type has to be specified explicitly using type information.
            AggregateOperator<Tuple2<String, Integer>> operator = strDataSource
                    .flatMap(new FlatMapFunction<String, String>() {
                        @Override
                        public void flatMap(String str, Collector<String> collector) throws Exception {
                            for (String s : str.split(CommonConstants.STR_BLANK)) {
                                collector.collect(s);
                            }
                        }
                    })
                    // flink 使用 lambda 时, 编译器类型推断存在问题
                    // 需要明确声明返回类型
                    //.returns(TypeInformation.of(String.class))
                    .returns(TypeInformation.of(String.class))
                    .map(new MapFunction<String, Tuple2<String, Integer>>() {
                        @Override
                        public Tuple2<String, Integer> map(String key) throws Exception {
                            return new Tuple2<String, Integer>(key, 1);
                        }
                    })
                    // 如果需要使用 lambda 表达式(或者类型声明不完整, 如 Tuple2 的类型没有明确),
                    // 为避免 could not be determined automatically, due to type erasure. 错误
                    // 需要通过以下方式明确类型:
                    //.returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                    .groupBy(0)
                    .sum(1);
            operator.print();

            //(hello,200000)

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

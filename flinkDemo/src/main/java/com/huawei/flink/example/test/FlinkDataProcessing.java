package com.huawei.flink.example.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class FlinkDataProcessing {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);

        // 添加自定义数据源，每秒生成约10000条数据
        DataStream<Tuple4<Long, String, String, Integer>> source = env.addSource(new DataSource());

        // 进行数据统计处理
        DataStream<Long> result = source
                .map(new ExtractLongValue())
                .timeWindowAll(Time.seconds(1))
                .reduce(new SumLongValues());

        // 打印结果到终端
        result.print();

        // 执行作业
        env.execute("Flink Data Processing Job");
    }

    // 自定义数据源，每秒生成约10000条四元组数据
    public static class DataSource extends RichSourceFunction<Tuple4<Long, String, String, Integer>> {
        private static final long serialVersionUID = 1L;
        private volatile boolean isRunning = true;
        private final Random random = new Random();
        private final String[] words = {"apple", "banana", "cherry", "date", "elderberry"};

        @Override
        public void run(SourceContext<Tuple4<Long, String, String, Integer>> ctx) throws Exception {
            long count = 0L;
            long startTime = System.currentTimeMillis();

            while (isRunning) {
                // 每秒生成约10000条数据
                if (System.currentTimeMillis() - startTime < 1000) {
                    for (int i = 0; i < 100; i++) {
                        ctx.collect(new Tuple4<>(
                                count++,
                                words[random.nextInt(words.length)],
                                "category" + random.nextInt(5),
                                random.nextInt(100)
                        ));
                        Thread.sleep(1); // 控制生成速率
                    }
                } else {
                    startTime = System.currentTimeMillis();
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // 提取Long类型字段的MapFunction
    public static class ExtractLongValue implements MapFunction<Tuple4<Long, String, String, Integer>, Long> {
        @Override
        public Long map(Tuple4<Long, String, String, Integer> value) throws Exception {
            return value.f0; // 返回第一个字段（Long类型）
        }
    }

    // 对Long值求和的ReduceFunction
    public static class SumLongValues implements ReduceFunction<Long> {
        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }
}    
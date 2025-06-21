package com.huawei.flink.example.checkpoint;


import com.huawei.flink.example.common.SimpleSourceWithCheckPoint;
import com.huawei.flink.example.common.WindowStatisticWithChk;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @program: BigdataStreamProject
 * @description:
 * @author: pkk
 * @create: 2022-09-30 10:46
 **/
public class FlinkProcessingTimeAPIChkMain {

    public static void main(String[] args) throws Exception
    {
//        String chkPath = ParameterTool.fromArgs(args).get("chkPath", "hdfs://hacluster/flink/checkpoints/");
        String chkPath = ParameterTool.fromArgs(args).get("chkPath", "hdfs://node1.itcast.cn:8020/flink-checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend((StateBackend) new FsStateBackend((chkPath)));
        env.enableCheckpointing(6000, CheckpointingMode.EXACTLY_ONCE);
        env.addSource(new SimpleSourceWithCheckPoint())
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(4), Time.seconds(1)))
                .apply(new WindowStatisticWithChk())
                .print();

        env.execute();

    }
}
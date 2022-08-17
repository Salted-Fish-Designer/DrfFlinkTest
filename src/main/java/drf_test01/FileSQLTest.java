package drf_test01;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.Test;

public class FileSQLTest {
    public static class Mylenth extends ScalarFunction {
        public int eval(String value)  {
            return value.length();
        }
    }

    public static void main(String[] args) {

        //Flink流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //开启CK
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //3.设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));

        //Flink表环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql("CREATE TABLE fs_table (\n" +
                "  logs STRING\n" +
                ") WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='file:/D:/Develop/Code/FlinkTest/DrfFlinkTest/src/textFlie',\n" +
                "  'format'='raw'\n" +
                ")");

        tableEnv.createTemporarySystemFunction("Mylenth", Mylenth.class);

        tableEnv.executeSql("select Mylenth(logs) from fs_table").print();

    }

}

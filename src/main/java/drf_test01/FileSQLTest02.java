package drf_test01;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;


public class FileSQLTest02 {

    // 自定义UDF函数
    public static class SetJsonObject extends ScalarFunction {
        public String eval(String data) {
            JSONObject object = JSON.parseObject(data);
            String time = object.get("_time").toString();
            return time;
        }
    }

    public static void main(String[] args) {
        //Flink流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //开启CK
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //Flink表环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //创建读取本地文件的表
        tableEnv.executeSql("CREATE TABLE MyUserTableWithFilepath (\n" +
                "  logs STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'file:\\D:\\Develop\\Code\\FlinkTest\\DrfFlinkTest\\src\\test20220711.txt',\n" +
                "  'format' = 'raw'\n" +
                ")");

        //注册自定义UDF
        tableEnv.createTemporarySystemFunction("set_json_object",SetJsonObject.class);

        //运行测试自定义UDF
        tableEnv.executeSql(" select set_json_object(logs) from MyUserTableWithFilepath ").print();
    }
}

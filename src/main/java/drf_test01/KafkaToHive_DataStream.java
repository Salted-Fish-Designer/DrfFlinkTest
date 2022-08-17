package drf_test01;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;


public class KafkaToHive_DataStream {
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

        // 创建hive catalog，连接hive
        String name = "myhive";
        String defaultDatabase = "test";
        String hiveConfDir = "/usr/local/service/hive/conf";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("10.16.0.10:9092")
                .setTopics("drf_test")
                .setGroupId("drf_test_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        Schema schema = Schema.newBuilder().column("f0", "STRING").build();
        Table tableA = tableEnv.fromDataStream(kafka_source, schema);

        tableA.printSchema();

//        tableEnv.executeSql("select * from " + tableA).print();


        tableEnv.executeSql("insert into test.drf_flink_test\n" +
                "select\n" +
                "f0,\n" +
                "JSON_VALUE(f0, '$.created_at')\n" +
                "from " + tableA);

    }
}

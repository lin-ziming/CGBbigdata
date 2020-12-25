package cn.tedu.flink.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 针对DataStreamAPI中Source和Sink的练习
 *
 * @author Haitao
 * @date 2020/12/25 15:51
 */
public class SourceSinkTest {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.获取数据源
//        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.65.161:9092");
        properties.setProperty("group.id", "test");
        DataStreamSource<String> source =
                env.addSource(new FlinkKafkaConsumer<>("flux", new SimpleStringSchema(), properties));
        //3.转化数据
        //hello --> (hello,1)
        source.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value,1);
            }
        }).keyBy(0).timeWindow(Time.seconds(5),Time.seconds(5)).sum(1)
        //4.输出结果
        .print();
        //5.触发程序执行（在DataStreamAPI中不管数据是否落地都需要触发程序执行）
        env.execute("SourceSinkTest");
    }
}

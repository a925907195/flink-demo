package stream;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;

import java.util.Properties;
import java.util.Random;

public class WriteIntoKafka {

    public static void main(String[] args) throws Exception {


        // 构造执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并发度
        env.setParallelism(1);
        // 解析运行参数
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        // 构造流图，将自定义Source生成的数据写入Kafka
        DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());



        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
        properties.setProperty("group.id", "test");



        FlinkKafkaProducer08 producer=new FlinkKafkaProducer08("sql", new org.apache.flink.streaming.util.serialization.SimpleStringSchema(), properties);

        messageStream.addSink(producer);

        // 调用execute触发执行
        env.execute();
    }

    // 自定义Source，每隔1s持续产生消息
    public static class SimpleStringGenerator implements SourceFunction<String> {
        static final String[] NAME = {"Carry", "Alen", "Mike", "Ian", "John", "Kobe", "James"};

        static final String[] SEX = {"MALE", "FEMALE"};

        static final int COUNT = NAME.length;

        boolean running = true;

        Random rand = new Random(47);

        @Override
        //rand随机产生名字，性别，年龄的组合信息
        public void run(SourceFunction.SourceContext<String> ctx) throws Exception {

            while (running) {

                int i = rand.nextInt(COUNT);

                int age = rand.nextInt(70);

                String sexy = SEX[rand.nextInt(2)];

                ctx.collect(NAME[i] + "," + age + "," + sexy);

                Thread.sleep(1000);

            }

        }

        @Override

        public void cancel() {

            running = false;

        }

    }

}
package com.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class Kafka010NumCountConsumer {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(35000); // 非常关键，一定要设置启动检查点！！
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<String>("test", new SimpleStringSchema(),
                properties);


//        consumer.setStartFromEarliest();
        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

        DataStream<Tuple3<String, Integer, String>> keyedStream = env
                .addSource(consumer)
                .flatMap(new MessageSplitter())
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .reduce(new ReduceFunction<Tuple3<String, Integer, String>>() {
                    @Override
                    public Tuple3<String, Integer, String> reduce(Tuple3<String, Integer, String> t0, Tuple3<String, Integer, String> t1) throws Exception {
                        String time0 = t0.getField(2);
                        String time1 = t1.getField(2);
                        Integer count0 = t0.getField(1);
                        Integer count1 = t1.getField(1);
                        return new Tuple3<>(t0.getField(0), count0 + count1, time0 +"|"+ time1);
                    }
                });

        keyedStream.writeAsText("/users/icezhang/Desktop/1.txt", FileSystem.WriteMode.OVERWRITE);
        System.out.println("-------------------------------------------------------");
        keyedStream.print();
        env.execute("Flink-Kafka num count");
    }

    private static class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {

        private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-hhmmss");

        /*
         * 再执行该函数，extractedTimestamp的值是extractTimestamp的返回值
         */
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
            if (lastElement != null && lastElement.contains(",")) {
                String[] parts = lastElement.split(",");
                if(parts.length==3) {
                    try {
                        return new Watermark(sdf.parse(parts[2]).getTime());
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }

            }
            return null;
        }

        /*
         * 先执行该函数，从element中提取时间戳
         * previousElementTimestamp 是当前的时间
         */
        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            if (element != null && element.contains(",")) {
                String[] parts = element.split(",");
                if (parts.length == 3) {
                    try {

                        return sdf.parse(parts[2]).getTime();
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
            return 0L;
        }
    }
    private static class MessageSplitter implements FlatMapFunction<String, Tuple3<String, Integer, String>> {


        @Override
        public void flatMap(String s, Collector<Tuple3<String, Integer, String>> collector) throws Exception {
            if (s != null && s.contains(",")) {
                String[] strs = s.split(",");
                if(strs.length==3) {
                    collector.collect(new Tuple3<>(strs[0], Integer.parseInt(strs[1]), strs[2]));
                }
            }
        }
    }
}

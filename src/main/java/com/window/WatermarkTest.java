package com.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.Properties;


public class WatermarkTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(2000);
        env.setParallelism(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<String>("aa", new SimpleStringSchema(), properties);

        DataStream<Tuple3<String, Long, Integer>> raw = env.addSource(consumer).map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String value) throws Exception {
                //每行输入数据形如: key1@0,key1@13等等，即在baseTimestamp的基础上加多少秒，作为当前event time
                String[] tmp = value.split("@");
                Long ts = Long.parseLong(tmp[1]);
                int car = Integer.parseInt(tmp[2]);
                return Tuple3.of(tmp[0], ts, car);
            }
        })
                //.assignTimestampsAndWatermarks(new MyTimestampExtractor(Time.seconds(10))); //允许10秒乱序，watermark为当前接收到的最大事件时间戳减10秒
                .assignTimestampsAndWatermarks(new CarDataWindow.CarTimestampExtractor());
        DataStream<String> window = raw.keyBy(0)
                //窗口都为自然时间窗口，而不是说从收到的消息时间为窗口开始时间来进行开窗，比如3秒的窗口，那么窗口一次是[0,3),[3,6)....[57,0),如果10秒窗口，那么[0,10),[10,20),...
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                // 允许5秒延迟
                //比如窗口[2018-03-03 03:30:00,2018-03-03 03:30:03),如果没有允许延迟的话，那么当watermark到达2018-03-03 03:30:03的时候，将会触发窗口函数并移除窗口，这样2018-03-03 03:30:03之前的数据再来，将被丢弃
                //在允许5秒延迟的情况下，那么窗口的移除时间将到watermark为2018-03-03 03:30:08,在watermark没有到达这个时间之前，你输入2018-03-03 03:30:00这个时间，将仍然会触发[2018-03-03 03:30:00,2018-03-03 03:30:03)这个窗口的计算
                .allowedLateness(Time.seconds(5))
                .apply(new WindowFunction<Tuple3<String, Long, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Long, Integer>> input, Collector<String> out) throws Exception {
                        LinkedList<Tuple3<String, Long, Integer>> data = new LinkedList<>();
                        for (Tuple3<String, Long, Integer> tuple2 : input) {
                            data.add(tuple2);
                        }
                        data.sort(new Comparator<Tuple3<String, Long, Integer>>() {
                            @Override
                            public int compare(Tuple3<String, Long, Integer> o1, Tuple3<String, Long, Integer> o2) {
                                return o1.f1.compareTo(o2.f1);
                            }
                        });
                        Integer sumCar = 0;
                        for (Tuple3<String, Long, Integer> tuple2 : input) {
                            sumCar = sumCar + tuple2.f2;
                        }
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String msg = String.format("key:%s,  window:[ %s  ,  %s ), elements count:%d, elements time range:[ %s  ,  %s ]", tuple.getField(0)
                                , format.format(new Date(window.getStart()))
                                , format.format(new Date(window.getEnd()))
                                , data.size()
                                , format.format(new Date(data.getFirst().f1))
                                , format.format(new Date(data.getLast().f1))
                        ) + "|||" + sumCar;
                        out.collect(msg);

                    }
                });
        window.print();

        env.execute();

    }

    public static class CarTimestampExtractor implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>> {


        Long maxOutOfOrderness = 3500L;
        Long currentMaxTimestamp = 0L;
        private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            System.out.println(String.format("call getCurrentWatermark======currentMaxTimestamp:%s  , lastEmittedWatermark:%s", format.format(new Date(currentMaxTimestamp)), format.format(new Date(currentMaxTimestamp - maxOutOfOrderness))));
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple3<String, Long, Integer> element, long l) {
            Long timestamp = element.f1;
            System.out.printf("" + timestamp);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }
}

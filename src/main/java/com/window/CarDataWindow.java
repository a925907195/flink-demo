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

public class CarDataWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(2000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<String>("aa", new SimpleStringSchema(), properties);

        DataStream<Tuple3<String, Long, Integer>> raw = env.addSource(consumer).map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String s) throws Exception {

                String[] tmp = s.split("@");
                Long ts = Long.parseLong(tmp[1]);
                int car = Integer.parseInt(tmp[2]);
                return Tuple3.of(tmp[0], ts, car);

            }
        }).assignTimestampsAndWatermarks(new CarTimestampExtractor());

        DataStream<String> window = raw.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
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
        env.execute("start exec");
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
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }
    }


}

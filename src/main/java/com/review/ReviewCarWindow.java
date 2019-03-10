package com.review;

import com.alibaba.fastjson.JSON;
import com.entity.CarEntity;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Properties;

public class ReviewCarWindow {

    public static void main(String[] args) throws  Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(2000);
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("zookeeper.connect","localhost:2181");
        properties.setProperty("group.id","test");

        FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<>("carwindow", new SimpleStringSchema(), properties);
        DataStream<Tuple3<String,Long, Integer>> raw = env.addSource(consumer).map(new MapFunction<String, Tuple3<String,Long,Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(String result) throws Exception {
                CarEntity carEntity = JSON.parseObject(result, CarEntity.class);
                return  new Tuple3<String,Long,Integer>(carEntity.getCarKind(),carEntity.getTimeStamp(),carEntity.getCarSum());
            }
        }).assignTimestampsAndWatermarks(new CarTimestampExtractor());

        DataStream<String> window = raw.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(5))
                .apply(new WindowFunction<Tuple3<String, Long, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Long, Integer>> input, Collector<String> out) throws Exception {
                        LinkedList<Tuple3<String, Long, Integer>> data = new LinkedList<>();

                        for (Tuple3<String,Long,Integer>item:input){
                            data.add(item);
                        }

                        Integer carSum=0;
                        for (Tuple3<String,Long,Integer> item:input){
                            carSum=carSum+item.f2;
                        }

                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String msg = String.format("key:%s,  window:[ %s  ,  %s ), elements count:%d, elements time range:[ %s  ,  %s ]", tuple.getField(0)
                                , format.format(new Date(window.getStart()))
                                , format.format(new Date(window.getEnd()))
                                , data.size()
                                , format.format(new Date(data.getFirst().f1))
                                , format.format(new Date(data.getLast().f1))
                        ) + "|||" + carSum;
                        out.collect(msg);
                    }
                });

        window.print();
        env.execute();


    }
    public static class CarTimestampExtractor implements AssignerWithPeriodicWatermarks<Tuple3<String,Long,Integer>>{


        Long maxOutOfOrderness=3500L;
        Long currentMaxTimeStamp=0L;


        @Nullable
        @Override
        public Watermark getCurrentWatermark() {


            System.out.println();
            return new Watermark(currentMaxTimeStamp-maxOutOfOrderness);

        }

        @Override
        public long extractTimestamp(Tuple3<String, Long, Integer> element, long l) {

            Long timeStamp=element.f1;
            currentMaxTimeStamp=Math.max(timeStamp,currentMaxTimeStamp);
            return timeStamp;
        }
    }

}

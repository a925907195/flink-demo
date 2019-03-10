package window;

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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class FlinkSessionWindow {


    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
        }).assignTimestampsAndWatermarks(new SessionTimeExtract());




        DataStream<String> window = raw.keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new WindowFunction<Tuple3<String,Long, Integer>, String, Tuple, TimeWindow>() {


                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String,Long, Integer>> iterable, Collector<String> collector) throws Exception {

                        Integer clickNum = 0;
                        for (Tuple3<String,Long, Integer> item : iterable) {
                            clickNum++;
                        }

                        String msg = "clickNum:" + clickNum;
                        collector.collect(msg);
                    }
                });

        window.print();
        env.execute();


    }



    public static class SessionTimeExtract implements AssignerWithPeriodicWatermarks<Tuple3<String,Long, Integer>>{

        private final Long maxOutOfOrderness=3500L;
        private Long currentMaxTimestamp=0L;
        private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            System.out.println(String.format("call getCurrentWatermark======currentMaxTimestamp:%s  , lastEmittedWatermark:%s", format.format(new Date(currentMaxTimestamp)), format.format(new Date(currentMaxTimestamp - maxOutOfOrderness))));
            return new Watermark(currentMaxTimestamp-maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple3<String,Long, Integer> element, long l) {

            long timestamp=element.f1;
            currentMaxTimestamp=Math.max(timestamp,currentMaxTimestamp);
            return timestamp;
        }
    }



}

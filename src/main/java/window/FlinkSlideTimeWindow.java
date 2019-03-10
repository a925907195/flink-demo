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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class FlinkSlideTimeWindow {

    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
        }).assignTimestampsAndWatermarks(new FlinkSessionWindow.SessionTimeExtract());


        DataStream<String> window = raw.keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new WindowFunction<Tuple3<String, Long, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Long, Integer>> iterable, Collector<String> collector) throws Exception {

                        Integer carSum = 0;
                        String carKey = "";
                        for (Tuple3<String, Long, Integer> item : iterable) {

                            carSum = carSum + item.f2;
                            carKey = item.f0;

                        }

                        String msg = "key:" + carKey + "    " + "carSum:  " + carSum;
                        collector.collect(msg);
                    }
                });

        window.print();
        env.execute();

    }
}

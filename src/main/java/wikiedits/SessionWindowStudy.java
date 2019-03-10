package wikiedits;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SessionWindowStudy {

    public static void main(String[] args)throws Exception {


        ParameterTool param = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(param);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//设置事件处理的时间选项
       // env.setParallelism(1);//设置并发执行

        env.enableCheckpointing(1000);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        boolean fileOutput = param.has("output");

        final List<Tuple3<String,Long,Integer>>input=new ArrayList<>();

        input.add(new Tuple3<>("a",1L,1));
        input.add(new Tuple3<>("b",1L,1));
        input.add(new Tuple3<>("b",3L,1));
        input.add(new Tuple3<>("b",5L,1));
        input.add(new Tuple3<>("c",6L,1));

        input.add(new Tuple3<>("a",10L,1));
        input.add(new Tuple3<>("c",11L,1));


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("zookeeper.connect", "127.0.0.1:2181");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer08<String> myConsumer = new FlinkKafkaConsumer08<String>("test", new SimpleStringSchema(),
                properties);

        DataStream<String> stream = env.addSource(myConsumer);
        //

        DataStreamSource<Tuple3<String, Long, Integer>> source = env.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {

            private static final long serialVersionID = 1L;

            @Override
            public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                for (Tuple3<String, Long, Integer> value : input) {
                    System.out.println(value+"|||||"+value.f1);
                    ctx.collectWithTimestamp(value, value.f1);
                    Tuple3<String,Long,Integer>singleData=new Tuple3<>();

                    ctx.emitWatermark(new Watermark(value.f1 - 1));

                }
                ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> aggregated = source.keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(1L)))
                .sum(2);
        if(fileOutput){
            aggregated.writeAsText(param.get("output"));
        }else{
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            aggregated.print();
        }
        env.execute();
    }
}

package window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 滑动窗口
 */
public class FlinkSlideWindowDemo {


    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        int windowSize=3;
        int slideSize=2;

        DataStreamSource<Tuple2<String, String>> inStream = env.addSource(new StreamDataSource());

        DataStream<Tuple2<String, String>> outStream = inStream.keyBy(0)
                .countWindow(windowSize, slideSize)
                .reduce(new ReduceFunction<Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> reduce(Tuple2<String, String> value1, Tuple2<String, String> value2) throws Exception {

                        return Tuple2.of(value1.f0,value1.f1+value2.f1);

                    }
                });


        outStream.print();
        env.execute();


    }




}

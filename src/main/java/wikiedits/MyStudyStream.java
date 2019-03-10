package wikiedits;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class MyStudyStream {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();//获取运行环境
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env.socketTextStream("localhost", 9999)//设置数据流源

                .flatMap(new Splitter())//数据流的操作   FlatMap的操作类
                .keyBy(0)//操作的对象是Tuple  并且tuple的第一个对象作为键
                .timeWindow(Time.seconds(5))//设置时间窗口   窗口的时间为5秒
                .sum(1);//在FlatMap的操作  循环一次 则对应+1

        dataStream.print();
        env.execute("Window WordCount");


    }

    public static class DataFilter implements FilterFunction<String>{


        @Override
        public boolean filter(String s) throws Exception {
            System.out.println(s);
            return true;
        }
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String,Integer>>{


        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for(String word:sentence.split(" ")){
                System.out.println(sentence);
                collector.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }


}

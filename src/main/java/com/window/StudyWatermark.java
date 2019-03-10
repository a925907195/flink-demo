package com.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class StudyWatermark {

    public static void main(String[] args) throws  Exception{

        int port=9010;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> text = env.socketTextStream("localhost", 9010 ,"\n");
        /**
         * 对输入的数据进行解析
         */
        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] arr = s.split(",");
                return new Tuple2<>(arr[0],Long.parseLong(arr[1]));
            }
        });

        //抽取timestamp 和 生成watermark
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

            long currentMaxTimestamp = 0L;
            final long maxOutOfOrderness = 10000L;

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                if(currentMaxTimestamp - maxOutOfOrderness>0){
                    System.out.println(currentMaxTimestamp - maxOutOfOrderness);
                }

                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {

                long timestamp = element.f1;

                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                long id = Thread.currentThread().getId();
                System.out.println("作者：fjsh 键值 :" + element.f0 + ",事件事件:[ " + sdf.format(element.f1) + " ],currentMaxTimestamp:[ "  +
                        sdf.format(currentMaxTimestamp) + " ],水印时间:[ " + sdf.format(getCurrentWatermark().getTimestamp()) + " ]");
                return timestamp;

            }
        });

        SingleOutputStreamOperator<String> window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /**
                         * 对window内的数据进行编排  保证数据的顺序
                         * @param tuple
                         * @param window
                         * @param input
                         * @param out
                         * @throws Exception
                         */
                        @Override
                        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                            String key = tuple.toString();
                            List<Long> arrarList = new ArrayList<>();
                            Iterator<Tuple2<String, Long>> it = input.iterator();
                            while (it.hasNext()) {
                                Tuple2<String, Long> next = it.next();
                                arrarList.add(next.f1);

                            }

                            Collections.sort(arrarList);
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                            String result = "\n 作者：fjsh 键值 : " + key + "\n              触发窗内数据个数 : " + arrarList.size() + "\n              触发窗起始数据： " + sdf.format(arrarList.get(0)) + "\n              触发窗最后（可能是延时）数据：" + sdf.format(arrarList.get(arrarList.size() - 1))
                                    + "\n              实际窗起始和结束时间： " + sdf.format(window.getStart()) + "《----》" + sdf.format(window.getEnd()) + " \n \n ";

                            out.collect(result);
                    }
                });

        window.print();

        env.execute("eventtime-watermark");


    }

}

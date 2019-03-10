package window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class StreamDataSourceWithTimestamp extends RichParallelSourceFunction<Tuple2<String, Long>> {
    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {

        Random random=new Random();
        int index = random.nextInt(3);

        ctx.collect(new Tuple2<>("car"+index,System.currentTimeMillis()));
        Thread.sleep(1200);

    }

    @Override
    public void cancel() {

    }
}

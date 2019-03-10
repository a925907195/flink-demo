package Watermarks.periodWatermarks;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 主要在processTime  eventTime中使用
 */

public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<Tuple2<String,Long>> {

    private Integer maxOutOfOrderness=3500;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis()-maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long l) {


        long timeStamp=element.f1;


        return timeStamp;
    }
}

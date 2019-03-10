package Watermarks.periodWatermarks;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * AssignWithPeriodicWatermarks 周期性的分配timestamp 和 生成 watermark(依赖元素自带的时间)
 *
 * watermark 产生的事件间隔 每N毫秒  是通过ExcutionConfig.setAutoWaterInterval()来定义的  每当分配器的 getCurrentWaterMark方法被调用的时  如果返回的watermark是非空并且大于上一个warkmark的话  一个新的watermark
 * 就会被发射
 */

public class BoundOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

    private final long maxOutOfOrderness=3500L;
    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp-maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Tuple2<String, Long> element, long l) {

        long timestamp=element.f1;
        currentMaxTimestamp=Math.max(timestamp,currentMaxTimestamp);
        return timestamp;
    }
}

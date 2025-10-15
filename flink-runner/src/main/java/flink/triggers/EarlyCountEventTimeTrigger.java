package flink.triggers;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.NumberSerializers;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class EarlyCountEventTimeTrigger extends Trigger<Object, TimeWindow> {

    private final ValueStateDescriptor<Long> elementsCountStateDescriptor = new ValueStateDescriptor<>("Elements count for which computation is triggered", LongSerializer.INSTANCE);
    private int intElementsCount;


    private EarlyCountEventTimeTrigger(int elementsCount) {
       // super();
        intElementsCount = elementsCount;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception
    {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        }
        ctx.registerEventTimeTimer(window.maxTimestamp());
        ValueState<Long> elementCountState = ctx.getPartitionedState(elementsCountStateDescriptor);


        if (elementCountState.value()==null)
        {
            elementCountState.update(0L);
        }
        long cnt=elementCountState.value().longValue();
        if ((cnt+1) % intElementsCount == 0)
        {
            elementCountState.update(0L);
            return TriggerResult.FIRE;
        }
        else
        {
            elementCountState.update(Long.valueOf(cnt+1));

            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        if (time == timeWindow.maxTimestamp() )
                return TriggerResult.FIRE;
        else
                return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow timeWindow, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(timeWindow.maxTimestamp());
        ctx.getPartitionedState(elementsCountStateDescriptor).clear();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window,
                        OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    public static EarlyCountEventTimeTrigger create(int cnt)
    {
        return new EarlyCountEventTimeTrigger(cnt);
    }
}

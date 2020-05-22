package com.example.loganalytics.pipeline.profiles;

import com.example.loganalytics.event.LogEvent;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Calendar;

public class LogEventTimestampAssigner extends BoundedOutOfOrdernessTimestampExtractor<LogEvent> {
    public LogEventTimestampAssigner(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(LogEvent logEvent) {
        long timestamp;

        if (logEvent != null) {
            timestamp = (long) logEvent.getField(LogEvent.TIMESTAMP_FIELD);
        } else {
            timestamp = Calendar.getInstance().getTimeInMillis();
        }
        return timestamp;
    }
}

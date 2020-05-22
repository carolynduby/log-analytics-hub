package com.example.loganalytics.pipeline.profiles;

import com.example.loganalytics.event.LogEvent;
import org.apache.flink.api.java.functions.KeySelector;

public class LogEventFieldKeySelector implements KeySelector<LogEvent, String> {

    private final String fieldName;

    public LogEventFieldKeySelector(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String getKey(LogEvent logEvent) {
        return (String) logEvent.getField(fieldName);
    }
}
package com.example.loganalytics.pipeline.profiles;

import com.example.loganalytics.event.LogEvent;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.ArrayList;
import java.util.List;

public class LogEventFieldKeySelector implements KeySelector<LogEvent, String> {

    private final List<String> fieldNames;

    public LogEventFieldKeySelector(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public String getKey(LogEvent logEvent) {
        List<String> fieldValues = new ArrayList<>();
        for (String fieldName : fieldNames) {
            String nextFieldValue = (String) logEvent.getField(fieldName);
            if (nextFieldValue != null) {
                fieldValues.add(nextFieldValue);
            }
        }
        return StringUtils.join(fieldValues, '-');
    }
}
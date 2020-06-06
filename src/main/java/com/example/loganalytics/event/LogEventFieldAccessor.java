package com.example.loganalytics.event;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.function.Function;

@ToString
@EqualsAndHashCode
public class LogEventFieldAccessor<T> implements Function<LogEvent, T> {
    private final String fieldName;
    private final String feature;

    public LogEventFieldAccessor(String fieldName, String feature) {
        this.fieldName = fieldName;
        this.feature = feature;
    }

    @Override
    public T apply(LogEvent logEvent) {
        try {
            //noinspection unchecked
            return (T) logEvent.getField(fieldName);
        } catch (ClassCastException e) {
            logEvent.reportError(fieldName, feature, e.getMessage());
            return null;
        }
    }
}

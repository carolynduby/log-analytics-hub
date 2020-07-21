package com.example.loganalytics.event;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@ToString
@EqualsAndHashCode
public class LogEventFieldConcatenate implements Function<LogEvent, String> {
    private final List<String> fieldNames;
    private final String feature;

    public LogEventFieldConcatenate(List<String> fieldNames, String feature) {
        this.fieldNames = fieldNames;
        this.feature = feature;
    }

    @Override
    public String apply(LogEvent logEvent) {
        List<String> values = new ArrayList<>();
        for (String fieldName : fieldNames) {
            try {
                String fieldValue = (String) logEvent.getField(fieldName);
                if (fieldValue != null) {
                    values.add(fieldValue);
                }
            } catch (ClassCastException e) {
                logEvent.reportError(fieldName, feature, e.getMessage());
                return null;
            }
        }
        return StringUtils.join(values, "-");

    }
}

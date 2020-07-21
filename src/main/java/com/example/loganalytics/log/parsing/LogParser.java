package com.example.loganalytics.log.parsing;

import com.example.loganalytics.event.LogEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Interface for all parsers that transform a raw log line with Input format to a map.
 *
 * @param <Input> Type of raw log event.
 */
public abstract class LogParser<Input> {

    private final Map<String, String> fieldRenames = new HashMap<>();
    private final Map<String, Function<Object, Object>> typeConversions = new HashMap<>();

    public LogEvent parse(Input rawLog) {
        LogEvent event = createEvent(rawLog);
        event.renameFields(fieldRenames);
        event.transformFields(typeConversions);

        return event;
    }

    public void addFieldRename(String originalFieldName, String newFieldName) {
        fieldRenames.put(originalFieldName, newFieldName);
    }

    public void addFieldTransformation(String fieldName, Function<Object, Object> transformation) {
        typeConversions.put(fieldName, transformation);
    }

    public abstract LogEvent createEvent(Input rawLog);
}

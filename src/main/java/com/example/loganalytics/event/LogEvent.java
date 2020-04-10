package com.example.loganalytics.event;


import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class LogEvent {
    public static final String ORIGINAL_STRING_FIELD_NAME = "original_string";
    public static final String FIELD_ERROR_MESSAGE = "'%s' = '%s': '%s' : %s";
    private final Map<String, Object> fields = new HashMap<>();
    private final Collection<String> errors = new HashSet<>();

    public LogEvent() {
    }

    public LogEvent(Map<String, Object> fields) {
        this.fields.putAll(fields);
    }

    public void setField(String fieldName, Object value) {
        fields.put(fieldName, value);
    }

    public Object getField(String fieldName) {
        return fields.get(fieldName);
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void reportError(String fieldName, String fieldValue, String feature, String error) {
        errors.add(String.format(FIELD_ERROR_MESSAGE, fieldName, fieldValue, feature, error));
    }

    public Collection<String> getErrors() {
        return errors;
    }
}

package com.example.loganalytics.event;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogEvent {
    public static final String ORIGINAL_STRING_FIELD_NAME = "original_string";
    public static final String FIELD_ERROR_MESSAGE = "'%s': '%s' : %s";
    public static final String NULL_FIELD_VALUE_ERROR_MESSAGE = "Field value is null.";
    public static final String FIELD_VALUE_TYPE_INCORRECT_ERROR_MESSAGE = "Expected field value type '%s' but got '%s'.";
    private Map<String, Object> fields = new HashMap<>();
    private Collection<String> errors = new HashSet<>();

    public LogEvent(Map<String, Object> fields) {
        this.fields.putAll(fields);
    }

    public void setField(String fieldName, Object value) {
        fields.put(fieldName, value);
    }

    public Object getField(String fieldName) {
        return fields.get(fieldName);
    }

    public void reportError(String fieldName, String feature, String error) {
        errors.add(String.format(FIELD_ERROR_MESSAGE, fieldName, feature, error));
    }

    public void reportError(LogEventFieldSpecification fieldSpecification, String error) {
        reportError(fieldSpecification.getFieldName(), fieldSpecification.getFeatureName(), error);
    }

    public <VT> VT getField(LogEventFieldSpecification specification, Class<VT> fieldValueType) {
        String fieldName = specification.getFieldName();
        String featureName = specification.getFeatureName();
        Object fieldValue = getField(specification.getFieldName());

        if (fieldValue == null) {
            if (specification.getIsRequired()) {
                reportError(fieldName, featureName, NULL_FIELD_VALUE_ERROR_MESSAGE);
            }
        } else if (!(fieldValueType.isInstance(fieldValue))) {
            reportError(fieldName, featureName, String.format(FIELD_VALUE_TYPE_INCORRECT_ERROR_MESSAGE,
                    fieldValueType.getSimpleName(),
                    fieldValue.getClass().getSimpleName()));
        } else {
            return fieldValueType.cast(fieldValue);
        }
        return null;
    }


    public void enrich(LogEventFieldSpecification baseFieldSpecification, String enrichmentFieldName, Object enrichmentFieldValue) {
        if (enrichmentFieldValue != null) {
            setField(String.format("%s.%s.%s", baseFieldSpecification.getFieldName(), baseFieldSpecification.getFeatureName(), enrichmentFieldName), enrichmentFieldValue);
        }
    }
    public void enrich(LogEventFieldSpecification baseFieldSpecification, Map<String, Object> enrichmentFields) {
        for(Map.Entry<String, Object> entry : enrichmentFields.entrySet()) {
            enrich(baseFieldSpecification, entry.getKey(), entry.getValue());
        }
    }
}

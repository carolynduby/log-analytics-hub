package com.example.loganalytics.event;


import com.example.loganalytics.event.serialization.TimeseriesEvent;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

@EqualsAndHashCode(callSuper = true)
@Data
public class LogEvent extends TimeseriesEvent {
    public static final String ORIGINAL_STRING_FIELD = "original_string";
    public static final String FIELD_ERROR_MESSAGE = "'%s': '%s' : %s";
    public static final String NULL_FIELD_VALUE_ERROR_MESSAGE = "Field value is null.";
    public static final String FIELD_VALUE_TYPE_INCORRECT_ERROR_MESSAGE = "Expected field value type '%s' but got '%s'.";
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String ERRORS_FIELD = "errors";
    public static final String TRANSFORMATION = "transformation";
    public static final String SOURCE_FIELD = "source";
    public static final String METADATA_DATASET_FIELD = "metadata.dataset";

    private Map<String, Object> fields = new HashMap<>();
    private Collection<String> errors;

    public LogEvent() {
        initErrorsField();
    }

    public LogEvent(Map<String, Object> fields) {
        this.fields.putAll(fields);
        //noinspection unchecked
        this.errors = (Collection<String>)fields.get(ERRORS_FIELD);
        if (errors == null) {
            initErrorsField();
        }
    }

    private void initErrorsField() {
        this.errors = new HashSet<>();
        this.fields.put(ERRORS_FIELD, errors);
    }

    @JsonAnySetter
    public void setField(String fieldName, Object value) {
        if (value != null) {
            fields.put(fieldName, value);
        }
    }

    public void addFields(Map<String, Object> fieldValues) {
        for(Map.Entry<String, Object> fieldValue : fieldValues.entrySet()) {
            setField(fieldValue.getKey(), fieldValue.getValue());
        }
    }

    @JsonAnyGetter
    public Map<String, Object> getFields() {
        return fields;
    }

    public Object getField(String fieldName) {
        return fields.get(fieldName);
    }

    public <VT> VT getField(String fieldName, String featureName, Class<VT> fieldValueType) {
        Object fieldValue = fields.get(fieldName);
        if (fieldValue != null) {
            if (!(fieldValueType.isInstance(fieldValue))) {
                reportError(fieldName, featureName, String.format(FIELD_VALUE_TYPE_INCORRECT_ERROR_MESSAGE,
                        fieldValueType.getSimpleName(),
                        fieldValue.getClass().getSimpleName()));
            } else {
                return fieldValueType.cast(fieldValue);
            }
        }
        return null;
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

        VT fieldValue = getField(specification.getFieldName(), specification.getFeatureName(), fieldValueType);
        if (fieldValue == null) {
            if (specification.getIsRequired()) {
                reportError(fieldName, featureName, NULL_FIELD_VALUE_ERROR_MESSAGE);
            }
        }
        return fieldValue;
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

    public void renameFields(Map<String, String> oldToNewFieldNames) {
        for(Map.Entry<String, String> fieldRename: oldToNewFieldNames.entrySet()) {
            setField(fieldRename.getValue(), fields.remove( fieldRename.getKey()));
        }
    }

    public void transformFields(Map<String, Function<Object, Object>> transformations) {
        for(Map.Entry<String, Function<Object, Object>> transformation : transformations.entrySet()) {
            String fieldName = transformation.getKey();
            Object originalFieldValue = fields.get(fieldName);
            if (originalFieldValue != null) {
                try {
                    Object newFieldValue = transformation.getValue().apply(originalFieldValue);
                    setField(fieldName, newFieldValue);
                } catch (Exception e) {
                    reportError(fieldName, TRANSFORMATION, e.getMessage());
                }
            }
        }

    }

    @JsonIgnore
    public long getTimestamp() {
        return (long)getField(TIMESTAMP_FIELD);
    }

    @JsonIgnore
    public String getSource() {
        return (String)getField(SOURCE_FIELD);
    }

    @JsonIgnore
    public Collection<String> getErrors() {
        return errors;
    }

    public void removeField(String fieldName) {
        fields.remove(fieldName);
    }

}

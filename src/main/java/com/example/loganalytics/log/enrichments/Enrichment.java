package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.event.LogEvent;

public abstract class Enrichment<VT> {
    public static final String NULL_FIELD_VALUE_ERROR_MESSAGE = "Field value is null.";
    public static final String FIELD_VALUE_TYPE_INCORRECT_ERROR_MESSAGE = "Expected field value type '%s' but got '%s'.";
    public static final String ENRICHMENT_FEATURE = "enrichment";
    protected final String featureName;
    private final Class<VT> enrichmentValueClass;

    protected Enrichment(String featureName, Class<VT> enrichmentValueClass) {
        this.featureName = ENRICHMENT_FEATURE.concat(".").concat(featureName);
        this.enrichmentValueClass = enrichmentValueClass;
    }

    protected VT getEnrichmentFieldValue(LogEvent event, String fieldName) {
        Object fieldValue = event.getField(fieldName);

        if (fieldValue == null) {
            event.reportError(fieldName, "null", featureName, NULL_FIELD_VALUE_ERROR_MESSAGE);
        } else if (!(enrichmentValueClass.isInstance(fieldValue))) {
            event.reportError(fieldName, fieldValue.toString(), featureName, String.format(FIELD_VALUE_TYPE_INCORRECT_ERROR_MESSAGE,
                    enrichmentValueClass.getSimpleName(),
                    fieldValue.getClass().getSimpleName()));
        } else {
            return enrichmentValueClass.cast(fieldValue);
        }
        return null;
    }

    public abstract void addEnrichment(LogEvent event, String fieldName);

    protected void addFieldValue(LogEvent event, String enrichmentFieldName, String logFieldEnding, Object fieldValue) {
        if (fieldValue != null) {
            event.setField(String.format("%s.%s.%s", enrichmentFieldName, featureName, logFieldEnding), fieldValue);
        }
    }
}

package com.example.loganalytics.log.sources;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;
import com.example.loganalytics.event.RawLog;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDataSource;
import com.example.loganalytics.log.parsing.LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class LogSource {
    private static final Logger LOG = LoggerFactory.getLogger(LogSource.class);

    private final String source;
    private final LogParser<String> parser;
    /**
     * an ordered list of enrichments to be applied a source
     */
    private final List<FieldEnrichment> enrichments = new ArrayList<>();

    public LogSource(String source, LogParser<String> parser) {
        this.source = source;
        this.parser = parser;
    }

    private void enrichEvent(LogEvent event) {
        if (event.getErrors().isEmpty()) {
            for (FieldEnrichment fieldEnrichment : enrichments) {
                fieldEnrichment.enrichEvent(event);
            }
        }
    }

    public LogEvent ingestEvent(String logText) {
        return ingestEvent(new RawLog(source, logText));
    }

    public LogEvent ingestEvent(RawLog rawEvent) {
        LogEvent event = parser.parse(rawEvent.getText());
        event.setField(LogEvent.SOURCE_FIELD, source);
        event.addFields(rawEvent.getMetadata());
        enrichEvent(event);

        return event;
    }

    public void configureFieldEnrichment(String fieldName, String feature, EnrichmentReferenceDataSource enrichmentReferenceDataSource) {
        configureFieldEnrichment(fieldName, feature, enrichmentReferenceDataSource, String.class, null);
    }

    public void configureFieldEnrichment(String fieldName, String feature, EnrichmentReferenceDataSource enrichmentReferenceDataSource,
                                         Class<?> fieldType, Predicate<LogEvent> applyEnrichment) {
        enrichments.add(new FieldEnrichment(new LogEventFieldSpecification(fieldName, feature, false), enrichmentReferenceDataSource, fieldType, applyEnrichment));
    }

    private static class FieldEnrichment {
        final LogEventFieldSpecification fieldSpecification;
        final EnrichmentReferenceDataSource enrichmentReferenceDataSource;
        final Class<?>   fieldEnrichmentClass;
        final Predicate<LogEvent> applyEnrichment;

        FieldEnrichment(LogEventFieldSpecification fieldSpecification, EnrichmentReferenceDataSource enrichment, Class<?> fieldEnrichmentClass, Predicate<LogEvent> applyEnrichment) {
            this.fieldSpecification = fieldSpecification;
            this.enrichmentReferenceDataSource = enrichment;
            this.fieldEnrichmentClass = fieldEnrichmentClass;
            this.applyEnrichment = applyEnrichment;
        }

        void enrichEvent(LogEvent logEvent) {

            if (applyEnrichment == null  || applyEnrichment.test(logEvent)) {
                Object fieldValue = logEvent.getField(fieldSpecification, fieldEnrichmentClass);
                try {
                    if (fieldValue != null) {
                        logEvent.enrich(fieldSpecification, enrichmentReferenceDataSource.lookup(fieldSpecification.getFeatureName(), fieldValue));
                    }
                } catch (Exception e) {
                    logEvent.reportError(fieldSpecification, e.getMessage());
                    LOG.error(String.format("Error enriching field '%s' with value '%s'", fieldSpecification.getFieldName(), fieldValue), e);
                }
            }
        }
    }
}

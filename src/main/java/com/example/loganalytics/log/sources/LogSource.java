package com.example.loganalytics.log.sources;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;
import com.example.loganalytics.log.enrichments.IpGeoEnrichment;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDataSource;
import com.example.loganalytics.log.parsing.LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LogSource<INPUT> {
    private static final Logger LOG = LoggerFactory.getLogger(LogSource.class);

    private final String sourceName;
    private final LogParser<INPUT> parser;
    /**
     * an ordered list of enrichments to be applied a source
     */
    private final List<FieldEnrichment> enrichments = new ArrayList<>();

    public LogSource(String sourceName, LogParser<INPUT> parser) {
        this.sourceName = sourceName;
        this.parser = parser;
    }

    private void enrichEvent(LogEvent event) {
        if (event.getErrors().isEmpty()) {
            for (FieldEnrichment fieldEnrichment : enrichments) {
                fieldEnrichment.enrichEvent(event);
            }
        }
    }

    public LogEvent ingestEvent(INPUT rawEvent) {
        LogEvent event = parser.parse(rawEvent);
        enrichEvent(event);

        return event;
    }

    public void configureFieldEnrichment(String fieldName, EnrichmentReferenceDataSource enrichmentReferenceDataSource) {
        enrichments.add(new FieldEnrichment(new LogEventFieldSpecification(fieldName, IpGeoEnrichment.GEOCODE_FEATURE, false), enrichmentReferenceDataSource));
    }

    private static class FieldEnrichment {
        LogEventFieldSpecification fieldSpecification;
        EnrichmentReferenceDataSource enrichmentReferenceDataSource;

        FieldEnrichment(LogEventFieldSpecification fieldSpecification, EnrichmentReferenceDataSource enrichment) {
            this.fieldSpecification = fieldSpecification;
            this.enrichmentReferenceDataSource = enrichment;
        }

        void enrichEvent(LogEvent logEvent) {

            String fieldValue = logEvent.getField(fieldSpecification, String.class);
            try {
                if (fieldValue != null) {
                    logEvent.enrich(fieldSpecification, enrichmentReferenceDataSource.lookup(fieldSpecification.getFeatureName(), fieldValue));
                }
            } catch (Exception e) {
                LOG.error(String.format("Error enriching field '%s' with value '%s'", fieldSpecification.getFieldName(), fieldValue), e);
            }
        }
    }
}

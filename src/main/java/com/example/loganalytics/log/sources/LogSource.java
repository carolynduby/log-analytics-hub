package com.example.loganalytics.log.sources;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.log.enrichments.Enrichment;
import com.example.loganalytics.log.parsing.LogParser;

import java.util.ArrayList;
import java.util.List;

public class LogSource<INPUT> {
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
                fieldEnrichment.enrichment.addEnrichment(event, fieldEnrichment.fieldName);
            }
        }
    }

    public LogEvent ingestEvent(INPUT rawEvent) {
        LogEvent event = parser.parse(rawEvent);
        enrichEvent(event);

        return event;
    }

    public void configureFieldEnrichment(String fieldName, Enrichment<?> enrichment) {
        enrichments.add(new FieldEnrichment(fieldName, enrichment));
    }

    private static class FieldEnrichment {
        String fieldName;
        Enrichment<?> enrichment;

        FieldEnrichment(String fieldName, Enrichment<?> enrichment) {
            this.fieldName = fieldName;
            this.enrichment = enrichment;
        }
    }
}

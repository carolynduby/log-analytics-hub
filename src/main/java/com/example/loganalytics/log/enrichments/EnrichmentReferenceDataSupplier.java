package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDataSource;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

@NoArgsConstructor
@Data
public class EnrichmentReferenceDataSupplier implements Supplier<Map<String, Object>> {
     private EnrichmentReferenceDataSource enrichmentReferenceDataSource;
     private String enrichmentKey;
     private LogEventFieldSpecification baseFieldSpecification;

    public EnrichmentReferenceDataSupplier(EnrichmentReferenceDataSource enrichmentReferenceDataSource, String enrichmentKey, LogEventFieldSpecification baseFieldSpecification) {
        this.enrichmentReferenceDataSource = enrichmentReferenceDataSource;
        this.baseFieldSpecification = baseFieldSpecification;
        this.enrichmentKey = enrichmentKey;
     }

    @Override
    public Map<String, Object> get() {
        try {
            return Collections.unmodifiableMap(enrichmentReferenceDataSource.lookup(baseFieldSpecification.getFeatureName(), enrichmentKey));
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }
}

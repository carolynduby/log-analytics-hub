package com.example.loganalytics.log.enrichments.reference;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EnrichmentReferenceData {
    private String enrichmentKey;
    private String enrichmentType;
    private Map<String, Object> enrichmentValues;
}
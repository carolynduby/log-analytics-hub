package com.example.loganalytics.log.enrichments.reference;

import java.util.Map;

public interface EnrichmentReferenceDataSource {
     Map<String, Object> lookup(String enrichmentReferenceData, Object fieldValue) throws Exception;
}

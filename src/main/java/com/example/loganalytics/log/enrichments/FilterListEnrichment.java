package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDataSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class FilterListEnrichment<ELEMENT> implements EnrichmentReferenceDataSource {

    public static final String FILTER_FEATURE_NAME = "filter";
    private final String filteredFieldName;
    private final Predicate<ELEMENT> filterPredicate;

    public FilterListEnrichment(String filteredFieldName, Predicate<ELEMENT> filterPredicate) {
        this.filteredFieldName = filteredFieldName;
        this.filterPredicate = filterPredicate;
    }

    @Override
    public Map<String, Object> lookup(String enrichmentReferenceData, Object fieldValue) {
        @SuppressWarnings("unchecked") List<ELEMENT> listFieldValue = (List<ELEMENT>) fieldValue;
        List<ELEMENT> filteredResult = new ArrayList<>();
        Map<String, Object> newFields = new HashMap<>();
        if (listFieldValue != null) {
            for (ELEMENT element : listFieldValue) {
                if (filterPredicate.test(element)) {
                    filteredResult.add(element);
                }
            }
            newFields.put(filteredFieldName, filteredResult);
        }

        return newFields;
    }
}

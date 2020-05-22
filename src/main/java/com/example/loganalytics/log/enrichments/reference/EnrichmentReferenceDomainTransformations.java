package com.example.loganalytics.log.enrichments.reference;

import com.google.common.base.Joiner;
import com.google.common.net.InternetDomainName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("UnstableApiUsage")
public class EnrichmentReferenceDomainTransformations {

    public static final String DOMAIN_PUBLIC_SUFFIX_ENDING = "public_suffix";
    public static final String DOMAIN_ABOVE_PUBLIC_SUFFIX_ENDING = "above_public_suffix";
    public static final String TOP_LEVEL_DOMAIN_ENDING = "top_level_domain";
    public static final String SECOND_LEVEL_DOMAIN_ENDING = "second_level_domain";
    public static final String DOMAIN_BREAKDOWN = "domain_breakdown";

    public final static EnrichmentReferenceDataSource domainBreakdown = (enrichmentReferenceData, domainFieldValueObject) -> {
        Map<String, Object> newFields = new HashMap<>();
        String domainFieldValue = (String) domainFieldValueObject;
        if (domainFieldValue != null && !domainFieldValue.isEmpty()) {
            InternetDomainName domainName = InternetDomainName.from(domainFieldValue);
            List<String> parts = domainName.parts();
            int domainPartsSize = parts.size();
            if (domainName.hasPublicSuffix()) {
                InternetDomainName suffix = domainName.publicSuffix();
                newFields.put(DOMAIN_PUBLIC_SUFFIX_ENDING, Joiner.on('.').join(suffix.parts()));
                int abovePublicSize = domainPartsSize - suffix.parts().size() - 1;
                if (abovePublicSize >= 0) {
                    newFields.put(DOMAIN_ABOVE_PUBLIC_SUFFIX_ENDING, parts.get(abovePublicSize));
                }
            }

            if (domainPartsSize >= 1) {
                newFields.put(TOP_LEVEL_DOMAIN_ENDING, parts.get(domainPartsSize - 1));
                if (domainPartsSize > 2) {
                    newFields.put(SECOND_LEVEL_DOMAIN_ENDING, parts.get(domainPartsSize - 2));
                }
            }
        }
        return newFields;
    };
}

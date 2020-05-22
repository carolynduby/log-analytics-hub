package com.example.loganalytics.log.enrichments.reference;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class EnrichmentReferenceDomainTransformationsTest {

    @Test
    public void publicSuffixSameAsTopLevel() throws Exception {
        Map<String, Object> fields = EnrichmentReferenceDomainTransformations.domainBreakdown.lookup("","www.google.com");
        verifyFields(fields, "com", "google", "com", "google");
    }

    @Test
    public void publicSuffixDifferentFromTopLevel() throws Exception {
        Map<String, Object> fields = EnrichmentReferenceDomainTransformations.domainBreakdown.lookup("","www.acorndomains.co.uk");
        verifyFields(fields, "uk", "co", "co.uk", "acorndomains");
    }

    @Test
    public void publicSuffixOnly() throws Exception {
        Map<String, Object> fields = EnrichmentReferenceDomainTransformations.domainBreakdown.lookup("","com");
        verifyFields(fields, "com", null, "com", null);
   }

    @Test
    public void emptyDomain() throws Exception {
        Map<String, Object> elements = EnrichmentReferenceDomainTransformations.domainBreakdown.lookup("", "");
        Assert.assertTrue(elements.isEmpty());
    }

    private void verifyFields(Map<String, Object> fields, String topLevelDomain, String secondLevelDomain, String publicSuffix, String abovePublicSuffix) {
        verifyField(fields, "top_level_domain", topLevelDomain);
        verifyField(fields, "second_level_domain", secondLevelDomain);
        verifyField(fields, "public_suffix", publicSuffix);
        verifyField(fields, "above_public_suffix", abovePublicSuffix);
    }

    private void verifyField(Map<String, Object> fields, String fieldName, String fieldValue) {
        if (fieldValue == null) {
            Assert.assertFalse(fields.containsKey(fieldName));
        } else {
            Assert.assertEquals(fieldValue, fields.get(fieldName));
        }
    }
}

package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class IpCityCountrySetEnrichmentTest extends IpGeoTest {

    @Test
    public void testSingleIpWithCountryNoCity() throws Exception {
        List<String> ipsAndHosts = Collections.singletonList("172.253.122.103");
        String[] expectedCities = {};
        String[] expectedCountries = { "US"};

        testIpList(ipsAndHosts, expectedCities, expectedCountries);
    }

    @Test
    public void testSingleIpWithCountryAndCity() throws Exception {
        List<String> ipsAndHosts = Collections.singletonList("35.168.30.147");
        String[] expectedCities = { "Ashburn"};
        String[] expectedCountries = { "US"};

        testIpList(ipsAndHosts, expectedCities, expectedCountries);
    }

    @Test
    public void testMultiIpAndHostMix() throws Exception {
        List<String> ipsAndHosts = Arrays.asList("35.168.30.147", "172.253.122.103", "www.google.com");
        String[] expectedCities = { "Ashburn"};
        String[] expectedCountries = { "US"};

        testIpList(ipsAndHosts, expectedCities, expectedCountries);
    }

    @Test
    public void testInternalIp() throws Exception {
        List<String> ipsAndHosts = Collections.singletonList("10.0.0.1");
        String[] expectedCities = { };
        String[] expectedCountries = { };

        testIpList(ipsAndHosts, expectedCities, expectedCountries);
    }

    @Test
    public void testSameIdTwice() throws Exception {
        List<String> ipsAndHosts = Arrays.asList("35.168.30.147", "35.168.30.147");
        String[] expectedCities = { "Ashburn" };
        String[] expectedCountries = { "US" };

        testIpList(ipsAndHosts, expectedCities, expectedCountries);
    }

    private void testIpList(List<String> ipsAndHosts, String[] expectedCities, String[] expectedCountries) throws Exception {
        IpCityCountrySetEnrichment enrichment = createCityCountryEnrichment();

        Map<String, Object> fields = enrichment.lookup("", ipsAndHosts);

        verifyGeos(fields, IpCityCountrySetEnrichment.UNIQUE_CITIES_FIELD_ENDING, expectedCities);
        verifyGeos(fields, IpCityCountrySetEnrichment.UNIQUE_COUNTRIES_FIELD_ENDING, expectedCountries);
    }

    private void verifyGeos(Map<String, Object> fields, String fieldName, String[] expectedValue) {
        @SuppressWarnings("unchecked") List<String> actualValue = (List<String>) fields.get(fieldName);
        Assert.assertArrayEquals(expectedValue, actualValue.toArray());
    }

    private IpCityCountrySetEnrichment createCityCountryEnrichment() throws IOException {
        EnrichmentReferenceFiles files = createEnrichmentReferenceFiles();

        return IpCityCountrySetEnrichment.create(files);
    }

}

package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class IpGeoEnrichmentTest extends IpGeoTest {

    @Test
    public void testNullCityState() throws Exception {
        testGeoEnrichment("172.253.122.103", "US", null, null, 37.751, -97.822);
    }

    @Test
    public void testAllFieldsPresent() throws Exception {
        testGeoEnrichment("35.168.30.147", "US", "Ashburn", "Virginia", 39.0481, -77.4728);
    }

    @Test
    public void testUnknownHost() throws Exception {
        final String badIPAddress = "blah.blah.blah";
        testGeoEnrichment(badIPAddress, null, null, null, null, null);
    }

    @Test
    public void testLocalIp() throws Exception {
        // local ips are legitimate addresses but don't have geocode info
        testGeoEnrichment("10.0.0.1", null, null, null, null, null);
    }

    private void testGeoEnrichment(String ip_address, String country, String city, String state, Double latitude, Double longitude) throws Exception {
        IpGeoEnrichment geoEnrichment = createGeoEnrichment();
        Map<String, Object> ipGeoValues = geoEnrichment.lookup(IpGeoEnrichment.GEOCODE_FEATURE, ip_address);
        Assert.assertEquals(country, ipGeoValues.get(IpGeoEnrichment.COUNTRY_FIELD_ENDING));
        Assert.assertEquals(city, ipGeoValues.get(IpGeoEnrichment.CITY_FIELD_ENDING));
        Assert.assertEquals(state, ipGeoValues.get(IpGeoEnrichment.STATE_FIELD_ENDING));
        Assert.assertEquals(latitude, ipGeoValues.get(IpGeoEnrichment.LATITUDE_FIELD_ENDING));
        Assert.assertEquals(longitude, ipGeoValues.get(IpGeoEnrichment.LONGITUDE_FIELD_ENDING));
    }

    private IpGeoEnrichment createGeoEnrichment() throws IOException {
        EnrichmentReferenceFiles files = createEnrichmentReferenceFiles();

        return IpGeoEnrichment.create(files);
    }

}

package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import com.maxmind.geoip2.DatabaseReader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IpGeoEnrichmentTest {

    @Test
    public void testNullCityState() throws Exception {
        testGeoEnrichment("172.253.122.103", "US", null, null, 37.751, -97.822);
    }

    @Test
    public void testAllFieldsPresent() throws Exception {
        testGeoEnrichment("35.168.30.147", "US", "Ashburn", "Virginia", 39.0481, -77.4728);
    }

    @Test(expected = Exception.class)
    public void testUnknownHost() throws Exception {
        final String badIPAddress = "blahblah.blah";
        testGeoEnrichment(badIPAddress, null, null, null, null, null);
    }

    /*
    @Test
    public void testUnsetField() throws IOException {
        IpGeoEnrichment geoEnrichment = createGeoEnrichment();
        LogEvent logEvent = new LogEvent();
        geoEnrichment.addEnrichment(logEvent, TEST_FIELD_NAME);
        LogEventTest.checkLogEventError(TEST_FIELD_NAME, "null", GEOCODE_FEATURE, Enrichment.NULL_FIELD_VALUE_ERROR_MESSAGE, logEvent.getErrors());
    }

    @Test
    public void testIncorrectFieldType() throws IOException {
        final IpGeoEnrichment geoEnrichment = createGeoEnrichment();
        final LogEvent logEvent = new LogEvent();
        final Integer fieldValueWithBadType = 15;
        logEvent.setField(TEST_FIELD_NAME, fieldValueWithBadType);
        geoEnrichment.addEnrichment(logEvent, TEST_FIELD_NAME);
        final Collection<String> errors = logEvent.getErrors();
        final String fieldMessage = String.format(Enrichment.FIELD_VALUE_TYPE_INCORRECT_ERROR_MESSAGE, String.class.getSimpleName(), Integer.class.getSimpleName());
        LogEventTest.checkLogEventError(TEST_FIELD_NAME, fieldValueWithBadType.toString(), GEOCODE_FEATURE, fieldMessage, errors);
    } */

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
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put(EnrichmentReferenceFiles.CORE_SITE_PROPERTY_NAME, "core-site.xml");
        paramMap.put(EnrichmentReferenceFiles.HDFS_SITE_PROPERTY_NAME, "hdfs-site.xml");
        paramMap.put(EnrichmentReferenceFiles.ENRICHMENT_GEO_PATH_PROPERTY_NAME, "file:///home/carolynduby/maxmind/GeoLite2-City_20200317/GeoLite2-City.mmdb");


        ParameterTool params = ParameterTool.fromMap(paramMap);
        EnrichmentReferenceFiles files = EnrichmentReferenceFiles.create(params);
        Assert.assertNotNull(files);
        DatabaseReader geoCityDb = files.getGeoCityDatabase();
        Assert.assertNotNull(geoCityDb);

        return IpGeoEnrichment.create(files);
    }
}

package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventTest;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import com.maxmind.geoip2.DatabaseReader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class IpGeoEnrichmentTest {

    private static final String TEST_FIELD_NAME = "dest_ip";
    private static final String GEOCODE_FEATURE = Enrichment.ENRICHMENT_FEATURE.concat(".").concat(IpGeoEnrichment.GEOCODE_FEATURE);

    @Test
    public void testNullCityState() throws IOException {
        testGeoEnrichment("172.253.122.103", "US", null, null, 37.751, -97.822, null);
    }

    @Test
    public void testAllFieldsPresent() throws IOException {
        testGeoEnrichment("35.168.30.147", "US", "Ashburn", "Virginia", 39.0481, -77.4728, null);
    }

    @Test
    public void testUnknownHost() throws IOException {
        final String badIPAddress = "blahblah.blah";
        testGeoEnrichment(badIPAddress, null, null, null, null, null, IpGeoEnrichment.UNKNOWN_HOST_ERROR_MESSAGE);
    }

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
    }

    @Test
    public void testLocalIp() throws IOException {
        // local ips are legitimate addresses but don't have geocode info
        testGeoEnrichment("10.0.0.1", null, null, null, null, null, null);
    }

    private void testGeoEnrichment(String ip_address, String country, String city, String state, Double latitude, Double longitude, String error) throws IOException {
        IpGeoEnrichment geoEnrichment = createGeoEnrichment();
        LogEvent logEvent = new LogEvent();
        logEvent.setField(TEST_FIELD_NAME, ip_address);
        geoEnrichment.addEnrichment(logEvent, TEST_FIELD_NAME);
        String enrichmentFieldBase = String.format("%s.%s.", TEST_FIELD_NAME, GEOCODE_FEATURE);
        Assert.assertEquals(country, logEvent.getField(enrichmentFieldBase.concat(IpGeoEnrichment.COUNTRY_FIELD_ENDING)));
        Assert.assertEquals(city, logEvent.getField(enrichmentFieldBase.concat(IpGeoEnrichment.CITY_FIELD_ENDING)));
        Assert.assertEquals(state, logEvent.getField(enrichmentFieldBase.concat(IpGeoEnrichment.STATE_FIELD_ENDING)));
        Assert.assertEquals(latitude, logEvent.getField(enrichmentFieldBase.concat(IpGeoEnrichment.LATITUDE_FIELD_ENDING)));
        Assert.assertEquals(longitude, logEvent.getField(enrichmentFieldBase.concat(IpGeoEnrichment.LONGITUDE_FIELD_ENDING)));
        Collection<String> errors = logEvent.getErrors();

        if (error == null) {
            Assert.assertTrue(errors.isEmpty());
        } else {
            LogEventTest.checkLogEventError(TEST_FIELD_NAME, ip_address, GEOCODE_FEATURE, error, errors);
        }
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

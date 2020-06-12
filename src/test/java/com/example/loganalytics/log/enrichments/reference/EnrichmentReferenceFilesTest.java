package com.example.loganalytics.log.enrichments.reference;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class EnrichmentReferenceFilesTest {

    public static ParameterTool createEnrichmentTestParameters() {
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put(EnrichmentReferenceFiles.CORE_SITE_PROPERTY_NAME, "core-site.xml");
        paramMap.put(EnrichmentReferenceFiles.HDFS_SITE_PROPERTY_NAME, "hdfs-site.xml");
        paramMap.put(EnrichmentReferenceFiles.ENRICHMENT_GEO_PATH_PROPERTY_NAME, "src/test/resources/maxmind/GeoLite2-City_20200609/GeoLite2-City.mmdb");

        return ParameterTool.fromMap(paramMap);
    }

    @Test
    public void testHappyPath() throws IOException, GeoIp2Exception {
        ParameterTool params = createEnrichmentTestParameters();
        EnrichmentReferenceFiles files = EnrichmentReferenceFiles.create(params);
        Assert.assertNotNull(files);
        DatabaseReader geoCityDb = files.getGeoCityDatabase();
        Assert.assertNotNull(geoCityDb);
        InetAddress ipAddress = InetAddress.getByName("172.253.122.103");
        Optional<CityResponse> cityResponse = geoCityDb.tryCity(ipAddress);
        Assert.assertNotNull(cityResponse);

        Assert.assertTrue(cityResponse.isPresent());
        String country = cityResponse.get().getCountry().getName();
        Assert.assertNotNull(country);
        Assert.assertTrue(country.length() > 0);
    }
}

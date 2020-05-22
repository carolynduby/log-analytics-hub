package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import com.maxmind.geoip2.DatabaseReader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IpGeoTest {
    protected EnrichmentReferenceFiles createEnrichmentReferenceFiles() throws IOException {
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put(EnrichmentReferenceFiles.CORE_SITE_PROPERTY_NAME, "core-site.xml");
        paramMap.put(EnrichmentReferenceFiles.HDFS_SITE_PROPERTY_NAME, "hdfs-site.xml");
        paramMap.put(EnrichmentReferenceFiles.ENRICHMENT_GEO_PATH_PROPERTY_NAME, "file:///home/carolynduby/maxmind/GeoLite2-City_20200317/GeoLite2-City.mmdb");


        ParameterTool params = ParameterTool.fromMap(paramMap);
        EnrichmentReferenceFiles files = EnrichmentReferenceFiles.create(params);
        Assert.assertNotNull(files);
        DatabaseReader geoCityDb = files.getGeoCityDatabase();
        Assert.assertNotNull(geoCityDb);
        return files;
    }
}

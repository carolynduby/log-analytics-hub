package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFilesTest;
import com.maxmind.geoip2.DatabaseReader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Assert;

import java.io.IOException;

public class IpGeoTest {
    protected EnrichmentReferenceFiles createEnrichmentReferenceFiles() throws IOException {

        ParameterTool params = EnrichmentReferenceFilesTest.createEnrichmentTestParameters();

        EnrichmentReferenceFiles files = EnrichmentReferenceFiles.create(params);
        Assert.assertNotNull(files);
        DatabaseReader geoCityDb = files.getGeoCityDatabase();
        Assert.assertNotNull(geoCityDb);
        return files;
    }
}

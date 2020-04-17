package com.example.loganalytics.log.enrichments.reference;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * NOTE: This test requires a Phoenix JDBC connection.  It is NOT a unit test.
 */
public class EnrichmentReferenceHbaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentReferenceHbaseTest.class);

    public static void main(String[] args) {

        try {
            Map<String, String> parameters = new HashMap<>();
            parameters.put(EnrichmentReferenceHbase.HBASE_URL_PROPERTY_NAME, "jdbc:phoenix:ec2-54-177-233-108.us-west-1.compute.amazonaws.com:2181");
            parameters.put(EnrichmentReferenceHbase.HBASE_USER_PROPERTY_NAME, "admin");
            parameters.put(EnrichmentReferenceHbase.HBASE_PASSWORD_PROPERTY_NAME, "admin");

            EnrichmentReferenceHbase enrichments = EnrichmentReferenceHbase.create(ParameterTool.fromMap(parameters));
            Map<String, Object> bad_site_enrichments = enrichments.lookup("malicious_host", "www.badsite.com");
            LOG.info("Bad site enrichments {}.", bad_site_enrichments.toString());
            Map<String, Object> good_site_enrichments = enrichments.lookup("malicious_host", "www.goodsite.com");
            LOG.info("Good site enrichments - should be empty {}", good_site_enrichments.toString());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

}
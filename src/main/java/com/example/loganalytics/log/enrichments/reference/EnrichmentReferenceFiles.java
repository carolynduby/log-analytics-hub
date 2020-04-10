package com.example.loganalytics.log.enrichments.reference;

import com.maxmind.geoip2.DatabaseReader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class EnrichmentReferenceFiles {

    public static final String CORE_SITE_PROPERTY_NAME = "hdfs.coresite";
    public static final String HDFS_SITE_PROPERTY_NAME = "hdfs.hdfssite";
    public static final String ENRICHMENT_GEO_PATH_PROPERTY_NAME = "enrichment.geo.path";
    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentReferenceFiles.class);
    private final Configuration hadoopConfiguration;
    private final String geoDatabasePath;

    private EnrichmentReferenceFiles(Configuration hadoopConfiguration, ParameterTool params) {
        this.hadoopConfiguration = hadoopConfiguration;
        this.geoDatabasePath = params.get(ENRICHMENT_GEO_PATH_PROPERTY_NAME, "hdfs://apps/loganalytics/enrichments/geo");
    }

    private static void setConfiguration(Configuration hadoopConfiguration, ParameterTool params, String propertyName) throws IllegalStateException {
        String path = params.get(propertyName);
        if (path != null) {
            hadoopConfiguration.addResource(new Path(path));
        } else {
            throw new IllegalStateException(String.format("Configuration file does not define property '%s'", propertyName));
        }
    }

    public static EnrichmentReferenceFiles create(ParameterTool params) throws IllegalStateException {
        Configuration hadoopConfiguration = new Configuration();
        setConfiguration(hadoopConfiguration, params, CORE_SITE_PROPERTY_NAME);
        setConfiguration(hadoopConfiguration, params, HDFS_SITE_PROPERTY_NAME);
        return new EnrichmentReferenceFiles(hadoopConfiguration, params);
    }

    public InputStream load(String referenceFilePath) throws IOException {
        Path path = new Path(referenceFilePath);
        FileSystem fs = path.getFileSystem(hadoopConfiguration);
        return fs.open(path);

    }

    public DatabaseReader getGeoCityDatabase() throws IOException {
        try {
            return new DatabaseReader.Builder(load(geoDatabasePath)).build();
        } catch (IOException e) {
            LOG.error(String.format("Unable to load geo city database '%s'", geoDatabasePath), e);
            throw e;
        }
    }
}

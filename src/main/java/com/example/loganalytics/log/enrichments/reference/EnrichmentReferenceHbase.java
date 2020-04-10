package com.example.loganalytics.log.enrichments.reference;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class EnrichmentReferenceHbase {

    public static final String HBASE_URL_PROPERTY_NAME = "enrichments.hbaseurl";
    public static final String HBASE_USER_PROPERTY_NAME = "enrichments.hbaseuser";
    public static final String HBASE_PASSWORD_PROPERTY_NAME = "enrichments.hbasepassword";
    public static final String ENRICHMENTS_SCHEMA = "CYBERREFERENCE";
    private static final String ENRICHMENTS_TABLE = "ENRICHMENT";
    private static final String REFERENCE_DATA_SET_COLUMN_NAME = "REFERENCE_DATA_SET";
    private static final String EVENT_FIELD_VALUE_DATA_SET_COLUMN_NAME = "EVENT_FIELD_VALUE";
    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentReferenceHbase.class);

    private final String hbaseJDBCUrl;
    private final Properties hbaseConnectionProperties;
    private final List<String> enrichmentColumnNames;
    private final String enrichmentLookupQuery;


    private EnrichmentReferenceHbase(String hbaseJDBCUrl, Properties hbaseConnectionProperties, List<String> enrichmentColumnNames) {
        this.hbaseJDBCUrl = hbaseJDBCUrl;
        this.hbaseConnectionProperties = hbaseConnectionProperties;
        this.enrichmentColumnNames = enrichmentColumnNames;
        String columnsQuery = StringUtils.join(enrichmentColumnNames, ", ");
        this.enrichmentLookupQuery = String.format("select %s from %s.%s where %s = ? and %s = ? limit 1", columnsQuery,
                ENRICHMENTS_SCHEMA, ENRICHMENTS_TABLE, REFERENCE_DATA_SET_COLUMN_NAME, EVENT_FIELD_VALUE_DATA_SET_COLUMN_NAME);
        LOG.info("Created enrichment connection with query '{}'", enrichmentLookupQuery);
    }

    private static Properties getDatabaseProperties(ParameterTool params) {
        String hbaseUser = params.getRequired(HBASE_USER_PROPERTY_NAME);
        String hbasePassword = params.getRequired(HBASE_PASSWORD_PROPERTY_NAME);
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", hbaseUser);
        connectionProperties.put("password", hbasePassword);
        connectionProperties.put("phoenix.schema.mapSystemTablesToNamespace", "true");
        connectionProperties.put("phoenix.schema.isNamespaceMappingEnabled", "true");

        return connectionProperties;
    }

    public static EnrichmentReferenceHbase create(ParameterTool params) throws SQLException {
        String hbaseJDBCUrl = params.getRequired(HBASE_URL_PROPERTY_NAME);

        LOG.info("Connecting to HBase {}", hbaseJDBCUrl);
        DriverManager.registerDriver(new org.apache.phoenix.jdbc.PhoenixDriver());
        Properties hbaseProperties = getDatabaseProperties(params);
        try (Connection connection = DriverManager.getConnection(hbaseJDBCUrl, hbaseProperties)) {
            LOG.info("Getting enrichment table metadata");
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(null, ENRICHMENTS_SCHEMA, ENRICHMENTS_TABLE, null);
            List<String> enrichmentColumnNames = new ArrayList<>();
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                if (!columnName.equals(REFERENCE_DATA_SET_COLUMN_NAME) && !columnName.equals(EVENT_FIELD_VALUE_DATA_SET_COLUMN_NAME)) {
                    enrichmentColumnNames.add(columnName);
                }
            }

            return new EnrichmentReferenceHbase(hbaseJDBCUrl, hbaseProperties, enrichmentColumnNames);
        }
    }

    public Map<String, Object> getEnrichmentValue(String enrichmentReferenceData, String fieldValue) throws SQLException {
        try (Connection connection = DriverManager.getConnection(hbaseJDBCUrl, hbaseConnectionProperties)) {
            Map<String, Object> enrichmentValues = new HashMap<>();
            LOG.debug("Getting enrichment '{}' for field value '{}'", enrichmentReferenceData, fieldValue);
            try (PreparedStatement lookupEnrichment = connection.prepareStatement(enrichmentLookupQuery)) {
                lookupEnrichment.setString(1, enrichmentReferenceData);
                lookupEnrichment.setString(2, fieldValue);
                try (ResultSet enrichmentResults = lookupEnrichment.executeQuery()) {
                    while (enrichmentResults.next()) {
                        for (String columnName : enrichmentColumnNames) {
                            Object enrichmentValueObject = enrichmentResults.getObject(columnName);
                            if (enrichmentValueObject != null) {
                                String enrichmentValue = enrichmentValueObject.toString();
                                enrichmentValues.put(columnName, enrichmentValue);
                                LOG.trace("Adding enrichment '{}' = '{}'", columnName, enrichmentValue);
                            }
                        }
                    }
                    return enrichmentValues;
                }
            }
        }
    }
}

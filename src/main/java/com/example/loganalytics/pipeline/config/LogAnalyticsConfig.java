package com.example.loganalytics.pipeline.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class LogAnalyticsConfig {
    public static final String KAFKA_PREFIX = "kafka.";
    private static final Logger LOG = LoggerFactory.getLogger(LogAnalyticsConfig.class);

    public static Properties readKafkaProperties(ParameterTool params) {
        Properties properties = new Properties();

        for (String key : params.getProperties().stringPropertyNames()) {
            if (key.startsWith(KAFKA_PREFIX)) {
                properties.setProperty(key.substring(KAFKA_PREFIX.length()), params.get(key));
            }
        }

        LOG.info("### Kafka parameters:");
        for (String key : properties.stringPropertyNames()) {
            LOG.info("Kafka param: {}={}", key, properties.get(key));
        }
        return properties;
    }

}

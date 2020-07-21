package com.example.loganalytics.pipeline.serialization;

import com.example.loganalytics.event.RawLog;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class RawLogSchema implements KafkaSerializationSchema<RawLog>, KafkaDeserializationSchema<RawLog> {

    public static final String METADATA_KAFKA_HEADER_PREFIX = "metadata.";
    public static final String SOURCE_METADATA_NAME = METADATA_KAFKA_HEADER_PREFIX.concat("source");

    @Override
    public boolean isEndOfStream(RawLog rawLog) {
        return false;
    }

    @Override
    public RawLog deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) {
        String source = null;
        Map<String, Object> metadata = new HashMap<>();
        for (Header header : consumerRecord.headers()) {
            String headerKey = header.key();
            if( headerKey != null && header.key().startsWith(METADATA_KAFKA_HEADER_PREFIX)) {
                if (SOURCE_METADATA_NAME.equals(headerKey)) {
                    source = new String(header.value());
                } else {
                    metadata.put(headerKey, new String(header.value()));
                }
            }
        }
        return new RawLog(source, new String(consumerRecord.value()), metadata);
    }

    @Override
    public TypeInformation<RawLog> getProducedType() {
        return TypeExtractor.getForClass(RawLog.class);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(RawLog rawLog, @Nullable Long aLong) {
        return null;
    }
}

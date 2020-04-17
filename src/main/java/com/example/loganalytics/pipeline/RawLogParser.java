package com.example.loganalytics.pipeline;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;
import com.example.loganalytics.event.serialization.JsonFormat;
import com.example.loganalytics.event.serialization.LogFormatException;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceHbase;
import com.example.loganalytics.log.sources.LogSource;
import com.example.loganalytics.log.sources.LogSources;
import com.example.loganalytics.pipeline.config.LogAnalyticsConfig;
import com.example.loganalytics.pipeline.enrichments.ReferenceDataEnrichmentFunction;
import com.example.loganalytics.pipeline.generators.SquidGenerator;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class RawLogParser {

    public static final String PARSER_ERRORS_TAG = "parser-errors";
    static final OutputTag<String> errorOutputTag = new OutputTag<String>(PARSER_ERRORS_TAG) {
    };
    private static final Logger LOG = LoggerFactory.getLogger(RawLogParser.class);
    private static LogSource<String> squidSource;

    private static void outputStreamToKafka(DataStream<String> stream, Properties producerConfig, ParameterTool params, String topicPropKey) {
        //noinspection deprecation
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(params.getRequired(topicPropKey), new SimpleStringSchema(),
                producerConfig);
        stream.addSink(producer);
    }

    public static void main(String[] args) throws Exception {

        String configFile = args[0];
        LOG.info("Reading config file  {}", configFile);
        ParameterTool params = ParameterTool.fromPropertiesFile(configFile);
        Properties kafkaProperties = LogAnalyticsConfig.readKafkaProperties(params);
        LogSources logSources = LogSources.create(params);
        squidSource = logSources.getSource(LogSources.SQUID_SOURCE_NAME);

        EnrichmentReferenceHbase hbaseReferenceDataSource = EnrichmentReferenceHbase.create(params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> rawLogEvents = env
                .addSource(new SquidGenerator())
                .name("SquidGenerator");

        // parse raw event text and convert to structured json
        SingleOutputStreamOperator<LogEvent> logEventStream = rawLogEvents.process(new LogParser());


        LogEventFieldSpecification fieldSpec = new LogEventFieldSpecification("ip_dst_addr", "malicious_ip", Boolean.FALSE);
        DataStream<LogEvent> enrichedEvents = AsyncDataStream.unorderedWait(logEventStream,
                new ReferenceDataEnrichmentFunction(hbaseReferenceDataSource, fieldSpec), 50, TimeUnit.SECONDS);

        SingleOutputStreamOperator<String> jsonEvents = enrichedEvents.process(new LogEventToJson());
        DataStream<String> parserErrors = jsonEvents.getSideOutput(errorOutputTag);

        outputStreamToKafka(jsonEvents, kafkaProperties, params, "logs.parsed.topic");
        outputStreamToKafka(parserErrors, kafkaProperties, params, "logs.errors.topic");

        env.execute("Squid log parser");
    }

    private static class LogParser extends ProcessFunction<String, LogEvent> {

        private static final Logger LOG = LoggerFactory.getLogger(LogParser.class);

        @Override
        public void processElement(String logText, Context context, Collector<LogEvent> parsedLogCollector) {
            LOG.info("Processing log {}", logText);
            parsedLogCollector.collect(squidSource.ingestEvent(logText));
         }
    }

    private static class LogEventToJson extends ProcessFunction<LogEvent, String> {

        private static final Logger LOG = LoggerFactory.getLogger(LogEventToJson.class);
        private static final JsonFormat jsonFormat = new JsonFormat();

        @Override
        public void processElement(LogEvent logEvent, Context context, Collector<String> jsonLogCollector) {
            try {
                jsonLogCollector.collect(jsonFormat.convert(logEvent));
            } catch (LogFormatException e) {
                LOG.error("Unable to convert log {} to json.", logEvent.toString());
            }
        }
    }
}

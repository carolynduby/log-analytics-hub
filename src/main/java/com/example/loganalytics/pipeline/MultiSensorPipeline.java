package com.example.loganalytics.pipeline;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.NetworkEvent;
import com.example.loganalytics.event.ProfileEvent;
import com.example.loganalytics.event.RawLog;
import com.example.loganalytics.event.serialization.JsonFormat;
import com.example.loganalytics.event.serialization.LogFormatException;
import com.example.loganalytics.event.serialization.TimeseriesEvent;
import com.example.loganalytics.log.sources.LogSource;
import com.example.loganalytics.log.sources.LogSources;
import com.example.loganalytics.pipeline.config.LogAnalyticsConfig;
import com.example.loganalytics.pipeline.profiles.LogEventFieldKeySelector;
import com.example.loganalytics.pipeline.profiles.LogEventTimestampAssigner;
import com.example.loganalytics.pipeline.profiles.dns.DnsHourProfileAggregator;
import com.example.loganalytics.pipeline.profiles.dns.DnsMinuteFingerprintAggregator;
import com.example.loganalytics.pipeline.serialization.RawLogSchema;
import com.example.loganalytics.profile.ProfileGroup;
import com.google.common.collect.Lists;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MultiSensorPipeline {

    public static final String PARSER_ERRORS_TAG = "parser-errors";
    private static final Logger LOG = LoggerFactory.getLogger(RawLogParser.class);

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

        //EnrichmentReferenceHbase hbaseReferenceDathbasaSource = EnrichmentReferenceHbase.create(params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000);

        DataStream<RawLog> rawLogEvents = env
                .addSource(new FlinkKafkaConsumer<>("raw-messages", new RawLogSchema(), kafkaProperties))
                .name("MultiSensorIngest");

        // parse raw event text and convert to structured json
        SingleOutputStreamOperator<LogEvent> enrichedEvents = rawLogEvents.process(new LogParser(params));

        // LogEventFieldSpecification fieldSpec = new LogEventFieldSpecification("dns.query", "malicious_ip", Boolean.FALSE);
        // DataStream<LogEvent> enrichedEvents = AsyncDataStream.unorderedWait(logEventStream,
        //         new ReferenceDataEnrichmentFunction(hbaseReferenceDataSource, fieldSpec), 50, TimeUnit.SECONDS).assignTimestampsAndWatermarks(new LogEventTimestampAssigner(Time.seconds(10)));

        DataStream<ProfileGroup<LogEvent>> dnsProfileEvents = enrichedEvents.assignTimestampsAndWatermarks(new LogEventTimestampAssigner(Time.seconds(10))).filter(logEvent -> LogSources.ZEEK_SOURCE_NAME.equals(logEvent.getSource()) || LogSources.BROTEXT_SOURCE_NAME.equals(logEvent.getSource())).keyBy(new LogEventFieldKeySelector(Lists.newArrayList(NetworkEvent.IP_SRC_ADDR_FIELD, LogEvent.METADATA_DATASET_FIELD))).
                window(TumblingEventTimeWindows.of(Time.minutes(1))).aggregate(new DnsMinuteFingerprintAggregator());
        DataStream<ProfileEvent> hourlyEvents = dnsProfileEvents.keyBy(ProfileGroup::getEntityKey).window(TumblingEventTimeWindows.of(Time.minutes(60))).aggregate(new DnsHourProfileAggregator());

        SingleOutputStreamOperator<String> jsonEnrichedEvents = enrichedEvents.process(new EventToJson<>());
        final OutputTag<String> outputTag = new OutputTag<String>(PARSER_ERRORS_TAG){};
        DataStream<String> parserErrors = enrichedEvents.getSideOutput(outputTag);

        outputStreamToKafka(jsonEnrichedEvents, kafkaProperties, params, "logs.parsed.topic");
        outputStreamToKafka(parserErrors, kafkaProperties, params, "logs.errors.topic");
        outputStreamToKafka(hourlyEvents.process(new EventToJson<>()), kafkaProperties, params, "profiles.topic");

        env.execute("Multisensor log parser");
    }


    private static class LogParser extends ProcessFunction<RawLog, LogEvent> {

        private static final Logger LOG = LoggerFactory.getLogger(LogParser.class);
        public static final String MULTI_SENSOR_PIPELINE_FEATURE = "MultiSensorPipeline";
        private final ParameterTool params;
        private transient LogSources logSources;
        private transient JsonFormat jsonFormat;
        private final OutputTag<String> outputTag = new OutputTag<String>(PARSER_ERRORS_TAG){};

        public LogParser(ParameterTool params) {
            this.params = params;
        }

        @Override
        public void processElement(RawLog logEvent, Context ctx, Collector<LogEvent> parsedLogCollector) {
            String logText = logEvent.getText();
            String logSourceName = logEvent.getSource();

            if (logSourceName == null ) {
                logSourceName = LogSources.ZEEK_SOURCE_NAME;
            }
            LOG.debug("Processing {}  log {}", logSourceName, logText);

            LogSource logSource = logSources.getSource(logSourceName);
            if (logSource != null) {
                parsedLogCollector.collect(logSource.ingestEvent(logEvent));
            } else {
                LogEvent event = new LogEvent();
                event.setField(LogEvent.ORIGINAL_STRING_FIELD, logText);
                event.setField(LogEvent.TIMESTAMP_FIELD, TimeseriesEvent.getCurrentTime());
                event.reportError(LogEvent.ORIGINAL_STRING_FIELD, MULTI_SENSOR_PIPELINE_FEATURE, String.format("No parser defined for source '%s'", logSourceName));
                try {
                    ctx.output(outputTag, jsonFormat.convert(event));
                } catch (Exception e) {
                    ctx.output(outputTag, String.format("{\"original_string\":\"%s\",\"errors\":[\"'original_string': '%s' : No parser defined for source '%s'\"],\"timestamp\":%d}", StringEscapeUtils.escapeJson(logText), MULTI_SENSOR_PIPELINE_FEATURE, logSourceName, event.getTimestamp()));
                }
            }
        }

        @Override
        public void open(Configuration configuration) throws Exception {
            this.logSources = LogSources.create(params);
            this.jsonFormat = new JsonFormat();

            super.open(configuration);
        }
    }

    private static class EventToJson<EVENT_TYPE> extends ProcessFunction<EVENT_TYPE, String> {

        private static final Logger LOG = LoggerFactory.getLogger(EventToJson.class);
        private static final JsonFormat jsonFormat = new JsonFormat();

        @Override
        public void processElement(EVENT_TYPE event, Context context, Collector<String> jsonLogCollector) {
            try {
                jsonLogCollector.collect(jsonFormat.convert(event));
            } catch (LogFormatException e) {
                LOG.error(String.format("Unable to convert log %s to json.", event.toString()), e);
            }
        }
    }
}

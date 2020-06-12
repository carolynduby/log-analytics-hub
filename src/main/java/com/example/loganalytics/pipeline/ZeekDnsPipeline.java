package com.example.loganalytics.pipeline;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.NetworkEvent;
import com.example.loganalytics.event.serialization.JsonFormat;
import com.example.loganalytics.event.serialization.LogFormatException;
import com.example.loganalytics.log.sources.LogSource;
import com.example.loganalytics.log.sources.LogSources;
import com.example.loganalytics.pipeline.config.LogAnalyticsConfig;
import com.example.loganalytics.pipeline.profiles.LogEventFieldKeySelector;
import com.example.loganalytics.pipeline.profiles.LogEventTimestampAssigner;
import com.example.loganalytics.event.ProfileEvent;
import com.example.loganalytics.pipeline.profiles.dns.DnsMinuteFingerprintAggregator;
import com.example.loganalytics.pipeline.profiles.dns.DnsHourProfileAggregator;
import com.example.loganalytics.profile.ProfileGroup;
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

@SuppressWarnings("ALL")
public class ZeekDnsPipeline {

    public static final String PARSER_ERRORS_TAG = "parser-errors";
    static final OutputTag<String> errorOutputTag = new OutputTag<String>(PARSER_ERRORS_TAG) {
    };
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

        //EnrichmentReferenceHbase hbaseReferenceDataSource = EnrichmentReferenceHbase.create(params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000);

        DataStream<String> rawLogEvents = env
                .addSource(new FlinkKafkaConsumer<>("zeek", new SimpleStringSchema(), kafkaProperties))
                .name("ZeekMessages");

        // parse raw event text and convert to structured json
        SingleOutputStreamOperator<LogEvent> enrichedEvents = rawLogEvents.process(new LogParser(params)).assignTimestampsAndWatermarks(new LogEventTimestampAssigner(Time.seconds(10)));


       // LogEventFieldSpecification fieldSpec = new LogEventFieldSpecification("dns.query", "malicious_ip", Boolean.FALSE);
       // DataStream<LogEvent> enrichedEvents = AsyncDataStream.unorderedWait(logEventStream,
       //         new ReferenceDataEnrichmentFunction(hbaseReferenceDataSource, fieldSpec), 50, TimeUnit.SECONDS).assignTimestampsAndWatermarks(new LogEventTimestampAssigner(Time.seconds(10)));

        DataStream<ProfileGroup<LogEvent>> dnsProfileEvents = enrichedEvents.keyBy(new LogEventFieldKeySelector(NetworkEvent.IP_SRC_ADDR_FIELD)).window(TumblingEventTimeWindows.of(Time.minutes(1))).aggregate(new DnsMinuteFingerprintAggregator());
        DataStream<ProfileEvent> hourlyEvents = dnsProfileEvents.keyBy(ProfileGroup::getEntityKey).window(TumblingEventTimeWindows.of(Time.minutes(5))).aggregate(new DnsHourProfileAggregator());

        SingleOutputStreamOperator<String> jsonEnrichedEvents = enrichedEvents.process(new EventToJson<>());
        DataStream<String> parserErrors = jsonEnrichedEvents.getSideOutput(errorOutputTag);

        outputStreamToKafka(jsonEnrichedEvents, kafkaProperties, params, "logs.parsed.topic");
        outputStreamToKafka(parserErrors, kafkaProperties, params, "logs.errors.topic");
        outputStreamToKafka(hourlyEvents.process(new EventToJson<>()), kafkaProperties, params, "profiles.topic");

        env.execute("Zeek log parser");
    }

    private static class LogParser extends ProcessFunction<String, LogEvent> {

        private static final Logger LOG = LoggerFactory.getLogger(LogParser.class);
        private ParameterTool params;
        private transient LogSource<String> zeekSource;

        public LogParser(ParameterTool params) {
            this.params = params;
        }

        @Override
        public void processElement(String logText, Context context, Collector<LogEvent> parsedLogCollector) {
            LOG.debug("Processing log {}", logText);
            parsedLogCollector.collect(zeekSource.ingestEvent(logText));
        }

        @Override
        public void open(Configuration configuration) throws Exception {
            LogSources logSources = LogSources.create(params);
            this.zeekSource = logSources.getSource(LogSources.ZEEK_SOURCE_NAME);

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

package com.example.loganalytics.log.parsing.json;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventTest;
import com.example.loganalytics.event.NetworkEvent;
import com.example.loganalytics.log.parsing.ParserTest;
import com.example.loganalytics.log.sources.LogSource;
import com.example.loganalytics.log.sources.LogSources;
import com.example.loganalytics.log.sources.LogSourcesTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class JsonParserTest  extends ParserTest {

    @Test
    public void testHappyPath() throws IOException {
        LogSource jsonSource = LogSourcesTest.createLogSource(LogSources.ZEEK_SOURCE_NAME);
        @SuppressWarnings("SpellCheckingInspection") final String validJsonLine = "{\"dns\": {\"ts\":1588709647.572953,\"uid\":\"Cwfli23YvRiruLucOh\",\"id.orig_h\":\"172.31.1.6\",\"id.orig_p\":13512,\"id.resp_h\":\"172.31.3.121\",\"id.resp_p\":53,\"proto\":\"udp\",\"trans_id\":41874,\"rtt\":0.002012968063354492,\"query\":\"track-front-prod-825173558.us-east-1.elb.amazonaws.com\",\"qclass\":1,\"qclass_name\":\"C_INTERNET\",\"qtype\":1,\"qtype_name\":\"A\",\"rcode\":0,\"rcode_name\":\"NOERROR\",\"AA\":false,\"TC\":false,\"RD\":true,\"RA\":true,\"Z\":0,\"answers\":[\"23.21.126.97\",\"50.17.222.11\",\"107.22.253.140\",\"54.243.169.159\",\"23.23.144.131\",\"23.23.222.251\",\"174.129.202.115\",\"23.21.127.5\"],\"TTLs\":[30.0,30.0,30.0,30.0,30.0,30.0,30.0,30.0],\"rejected\":false}}";
        LogEvent event = jsonSource.ingestEvent(validJsonLine);
        verifyField(event, NetworkEvent.IP_SRC_ADDR_FIELD, "172.31.1.6");
        verifyField(event, NetworkEvent.IP_SRC_PORT_FIELD, 13512L);
        verifyField(event, NetworkEvent.IP_DST_ADDR_FIELD, "172.31.3.121");
        verifyField(event, NetworkEvent.IP_DST_PORT_FIELD, 53L);
        verifyField(event, LogEvent.TIMESTAMP_FIELD, 1588709647572L);

        verifyOriginalString(event, validJsonLine);
    }

    @Test
    public void testBadFormat() throws IOException {
        LogSource logSource = LogSourcesTest.createLogSource(LogSources.ZEEK_SOURCE_NAME);
        final String invalidLogLine = "badformat";
        LogEvent event = logSource.ingestEvent(invalidLogLine);
        verifyOriginalString(event, invalidLogLine);

        LogEventTest.checkLogEventError(LogEvent.ORIGINAL_STRING_FIELD, JsonParser.JSON_PARSER_FEATURE,
                String.format(JsonParser.JSON_MAPPING_ERROR_MESSAGE, "Expected value at 1:1"), event.getErrors());
    }

    private void verifyField(LogEvent event, String fieldName, Object expectedFieldValue) {
        Object actualFieldValue = event.getField(fieldName);
        Assert.assertEquals(expectedFieldValue, actualFieldValue);
    }
}

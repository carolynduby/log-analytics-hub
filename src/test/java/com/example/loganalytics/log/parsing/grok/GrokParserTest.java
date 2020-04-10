package com.example.loganalytics.log.parsing.grok;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventTest;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFilesTest;
import com.example.loganalytics.log.sources.LogSource;
import com.example.loganalytics.log.sources.LogSources;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class GrokParserTest {

    @Test
    public void testHappyPath() throws IOException {
        LogSource<String> squidSource = createLogSource();
        final String validSquidLogLine = "1528766038.123  70328 75.133.181.135 TCP_TUNNEL/200 420 CONNECT data.cnn.com:443 - HIER_DIRECT/2.20.22.7 -";
        LogEvent event = squidSource.ingestEvent(validSquidLogLine);
        Object elapsedTime = event.getField("elapsed");
        Assert.assertEquals("70328", elapsedTime);
        verifyOriginalString(event, validSquidLogLine);
    }

    @Test
    public void testBadFormat() throws IOException {
        LogSource<String> squidSource = createLogSource();
        final String invalidSquidLogLine = "badformat";
        LogEvent event = squidSource.ingestEvent(invalidSquidLogLine);
        verifyOriginalString(event, invalidSquidLogLine);

        LogEventTest.checkLogEventError(LogEvent.ORIGINAL_STRING_FIELD_NAME, LogEvent.ORIGINAL_STRING_FIELD_NAME, GrokParser.GROK_PARSER_FEATURE, GrokParser.GROK_MISMATCH_ERROR_MESSAGE, event.getErrors());
    }

    private void verifyOriginalString(LogEvent event, String expectedOriginalString) {
        Object originalString = event.getField(LogEvent.ORIGINAL_STRING_FIELD_NAME);
        Assert.assertEquals(expectedOriginalString, originalString);
    }

    private LogSource<String> createLogSource() throws IOException {
        LogSources logSources = LogSources.create(EnrichmentReferenceFilesTest.createEnrichmentTestParameters());
        return logSources.getSource(LogSources.SQUID_SOURCE_NAME);
    }
}

package com.example.loganalytics.log.parsing;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFilesTest;
import com.example.loganalytics.log.sources.LogSource;
import com.example.loganalytics.log.sources.LogSources;
import org.junit.Assert;

import java.io.IOException;

public class ParserTest {
    public static LogSource<String> createLogSource(String sourceName) throws IOException {
        LogSources logSources = LogSources.create(EnrichmentReferenceFilesTest.createEnrichmentTestParameters());
        return logSources.getSource(sourceName);
    }

    protected void verifyOriginalString(LogEvent event, String expectedOriginalString) {
        Object originalString = event.getField(LogEvent.ORIGINAL_STRING_FIELD_NAME);
        Assert.assertEquals(expectedOriginalString, originalString);
    }
}

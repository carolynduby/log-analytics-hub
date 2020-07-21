package com.example.loganalytics.log.parsing;

import com.example.loganalytics.event.LogEvent;
import org.junit.Assert;

public class ParserTest {

    protected void verifyOriginalString(LogEvent event, String expectedOriginalString) {
        Object originalString = event.getField(LogEvent.ORIGINAL_STRING_FIELD);
        Assert.assertEquals(expectedOriginalString, originalString);
    }
}

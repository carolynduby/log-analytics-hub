package com.example.loganalytics.event;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class LogEventTest {

    public static void checkLogEventError(String fieldName, String feature, String error, Collection<String> errors) {
        Assert.assertEquals(1, errors.size());
        String expectedMessage = String.format(LogEvent.FIELD_ERROR_MESSAGE, fieldName, feature, error);
        Assert.assertEquals(expectedMessage, errors.iterator().next());
    }

    @Test
    public void testFieldsWithDefaultConstructor() {
        final String originalStringValue = "raw log message one";

        final LogEvent event = new LogEvent();
        Assert.assertNull(event.getField(LogEvent.ORIGINAL_STRING_FIELD_NAME));
        event.setField(LogEvent.ORIGINAL_STRING_FIELD_NAME, originalStringValue);

        verifyFieldValue(originalStringValue, event);
    }

    @Test
    public void testFieldsWithMapConstructor() {
        final String originalStringValue = "raw log message two";
        final Map<String, Object> fields = new HashMap<>();

        fields.put(LogEvent.ORIGINAL_STRING_FIELD_NAME, originalStringValue);

        final LogEvent event = new LogEvent(fields);
        verifyFieldValue(originalStringValue, event);
    }

    @Test
    public void testReportError() {
        LogEvent event = new LogEvent();
        String fieldName = "test_field";
        String feature = "TEST_FEATURE";
        String error = "this is only a test";
        event.reportError(fieldName, feature, error);
        checkLogEventError(fieldName, feature, error, event.getErrors());
    }

    private void verifyFieldValue(String originalStringValue, LogEvent event) {
        Assert.assertEquals(originalStringValue, event.getField(LogEvent.ORIGINAL_STRING_FIELD_NAME));
        Map<String, Object> eventFields = event.getFields();
        Assert.assertEquals(1, eventFields.size());
        Assert.assertEquals(originalStringValue, eventFields.get(LogEvent.ORIGINAL_STRING_FIELD_NAME));
        Assert.assertTrue(event.getErrors().isEmpty());
    }

}

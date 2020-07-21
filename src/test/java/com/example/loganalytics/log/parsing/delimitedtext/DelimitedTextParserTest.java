package com.example.loganalytics.log.parsing.delimitedtext;

import com.example.loganalytics.event.LogEvent;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

public class DelimitedTextParserTest {

    private static final String SET_COLUMN_NAME = "set_column";
    private static final String UNSET_COLUMN_NAME = "unset_column";
    private static final String EMPTY_COLUMN_NAME = "empty_column";
    public static final String[] COLUMN_NAMES = new String[]{SET_COLUMN_NAME, UNSET_COLUMN_NAME, EMPTY_COLUMN_NAME};

    @Test
    public void testDefaultSettings() {
        DelimitedTextParser  parser = new DelimitedTextParser(COLUMN_NAMES);

        testParseSuccess(parser);
    }

    @Test
    public void testDefaultOverrides() {
        DelimitedTextParser  parser = new DelimitedTextParser(COLUMN_NAMES);
        parser.setDelimiter(",");
        parser.setEmptyFieldValue("blank");
        parser.setUnsetFieldValue("null");

        testParseSuccess(parser);
    }

    @Test
    public void testWrongNumberOfColumns() {
        DelimitedTextParser parser = new DelimitedTextParser(new String[] {});
        String eventString = "a\tb\tc";
        LogEvent event = parser.parse(eventString);

        Collection<String> errors = event.getErrors();
        Assert.assertEquals(1, errors.size());
        String expectedErrorMessage = String.format(DelimitedTextParser.INCORRECT_NUMBER_OF_FIELDS, 3, 0);
        Assert.assertTrue( errors.iterator().next().endsWith(expectedErrorMessage));
    }

    private void testParseSuccess(DelimitedTextParser parser) {

        String unsetValue = parser.getUnsetFieldValue();
        String emptyValue = parser.getEmptyFieldValue();
        String setValue = "my_value";
        String eventString = String.join(parser.getDelimiter(), setValue, unsetValue, emptyValue);

        LogEvent event = parser.parse(eventString);

        Assert.assertEquals(setValue, event.getField(SET_COLUMN_NAME));
        Assert.assertNull(unsetValue, event.getField(UNSET_COLUMN_NAME));
        Assert.assertTrue(((String)event.getField(EMPTY_COLUMN_NAME)).isEmpty());
        Assert.assertEquals(eventString, event.getField(LogEvent.ORIGINAL_STRING_FIELD));
        Assert.assertTrue(event.getErrors().isEmpty());
    }
}

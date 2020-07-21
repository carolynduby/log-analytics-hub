package com.example.loganalytics.log.parsing.delimitedtext;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.log.parsing.LogParser;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper=true)
@Data
public class DelimitedTextParser extends LogParser<String> {

    public static final String DELIMITED_TEXT_PARSER_FEATURE = "DELIMITED_TEXT_PARSER";
    public static final String INCORRECT_NUMBER_OF_FIELDS = "Incorrect number of fields.  Got %d.  Expected %d.";
    private String delimiter = "\t";
    private String emptyFieldValue = "(empty)";
    private String unsetFieldValue = "-";
    private final String[] fieldNames;

    public DelimitedTextParser(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public LogEvent createEvent(String rawLog) {
        String[] fieldValues = rawLog.split(delimiter);
        LogEvent logEvent = new LogEvent();
        logEvent.setField(LogEvent.ORIGINAL_STRING_FIELD, rawLog);
        if (fieldValues.length == fieldNames.length) {
            for (int i = 0; i < fieldValues.length; i++) {
                String value = fieldValues[i];
                if (!unsetFieldValue.equals(value)) {
                    if (emptyFieldValue.equals(value)) {
                        value = "";
                    }
                    logEvent.setField(fieldNames[i], value);
                }
            }
        } else {
            logEvent.reportError(LogEvent.ORIGINAL_STRING_FIELD, DELIMITED_TEXT_PARSER_FEATURE, String.format(INCORRECT_NUMBER_OF_FIELDS, fieldValues.length, fieldNames.length));
        }

        return logEvent;
    }
}

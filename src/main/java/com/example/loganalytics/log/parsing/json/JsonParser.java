package com.example.loganalytics.log.parsing.json;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.log.parsing.LogParser;
import com.github.wnameless.json.flattener.FlattenMode;
import com.github.wnameless.json.flattener.JsonFlattener;

import java.util.Map;
import java.util.function.Function;

public class JsonParser implements LogParser<String> {
    public static final String JSON_MAPPING_ERROR_MESSAGE = "Event log json format could not be converted to a log event. Exception message '%s'";
    public static final String JSON_PARSER_FEATURE = "JSON_PARSER";
    private final Map<String, String> fieldRenames;
    private final Map<String, Function<Object, Object>> fieldTypeConversions;

    public JsonParser(Map<String, String> fieldRenames, Map<String, Function<Object, Object>> fieldTypeConversions) {
        this.fieldRenames = fieldRenames;
        this.fieldTypeConversions = fieldTypeConversions;
    }

    @Override
    public LogEvent parse(String rawLog) {
        LogEvent event;

        try {
            Map<String,Object> fields = new JsonFlattener(rawLog).withFlattenMode(FlattenMode.KEEP_PRIMITIVE_ARRAYS).flattenAsMap();
            event = new LogEvent(fields);
            event.renameFields(fieldRenames);
            event.convertFieldTypes(fieldTypeConversions);
        } catch (Exception e) {
            event = new LogEvent();
            event.reportError(LogEvent.ORIGINAL_STRING_FIELD_NAME, JSON_PARSER_FEATURE, String.format(JSON_MAPPING_ERROR_MESSAGE, e.getMessage()));
        }
        event.setField(LogEvent.ORIGINAL_STRING_FIELD_NAME, rawLog);
        return event;
    }
}

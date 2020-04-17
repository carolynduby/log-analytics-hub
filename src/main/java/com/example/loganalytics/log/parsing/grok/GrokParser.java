package com.example.loganalytics.log.parsing.grok;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.log.parsing.LogParser;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;

import java.util.Map;

public class GrokParser implements LogParser<String> {
    public static final String GROK_MISMATCH_ERROR_MESSAGE = "Event log did not match Grok expression for source type";
    public static final String GROK_PARSER_FEATURE = "GROK_PARSER";
    private final Grok grok;

    public GrokParser(String topLevelExpression, Map<String, String> grokExpressions) {
        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();
        grokCompiler.register(grokExpressions);
        this.grok = grokCompiler.compile(String.format("%%{%s}", topLevelExpression));
    }

    @Override
    public LogEvent parse(String rawLog) {
        Match match = grok.match(rawLog);
        final Map<String, Object> eventFields = match.capture();
        LogEvent event;
        if (eventFields.isEmpty()) {
            event = new LogEvent();
            event.reportError(LogEvent.ORIGINAL_STRING_FIELD_NAME, GROK_PARSER_FEATURE, GROK_MISMATCH_ERROR_MESSAGE);
        } else {
            event = new LogEvent(eventFields);
        }
        event.setField(LogEvent.ORIGINAL_STRING_FIELD_NAME, rawLog);

        return event;
    }
}
